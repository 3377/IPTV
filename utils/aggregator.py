import asyncio
import copy
from collections import defaultdict
from logging import INFO

import utils.constants as constants
from utils.channel import sort_channel_result, generate_channel_statistic
from utils.channel import write_channel_to_file
from utils.config import config
from utils.tools import get_logger


class ResultAggregator:
    """
    Aggregates test results and periodically writes sorted views to files.
    """

    def __init__(
            self,
            base_data: dict,
            first_channel_name: str = None,
            ipv6_support: bool = True,
            write_interval: float = 2.0,
            min_items_before_flush: int = 1,
            sort_logger=None,
            stat_logger=None,
    ):
        self.base_data = base_data
        self.test_results = defaultdict(lambda: defaultdict(list))
        self._dirty = False
        self._dirty_count = 0
        self._stopped = True
        self._task: asyncio.Task | None = None
        self.write_interval = write_interval
        self.first_channel_name = first_channel_name
        self.ipv6_support = ipv6_support
        self.sort_logger = sort_logger or get_logger(constants.result_log_path, level=INFO, init=True)
        self.stat_logger = stat_logger or get_logger(constants.statistic_log_path, level=INFO, init=True)
        self.is_last = False
        self._lock = asyncio.Lock()
        self._min_items_before_flush = min_items_before_flush
        self._pending_flush_task: asyncio.Task | None = None

    def add_item(self, cate: str, name: str, item: dict, is_channel_last: bool = False, is_last: bool = False):
        """
        Add a new test result item.
        """
        self.test_results[cate][name].append(item)
        self._dirty = True
        self._dirty_count += 1
        self.is_last = is_last

        try:
            self.sort_logger.info(
                f"Name: {name}, URL: {item.get('url')}, From: {item.get('origin')}, "
                f"IPv_Type: {item.get('ipv_type')}, Location: {item.get('location')}, ISP: {item.get('isp')}, "
                f"Date: {item.get('date')}, Delay: {item.get('delay') or -1} ms, "
                f"Speed: {(item.get('speed') or 0):.2f} M/s, Resolution: {item.get('resolution')}"
            )
        except Exception:
            pass

        if is_channel_last:
            try:
                generate_channel_statistic(self.stat_logger, cate, name, self.test_results[cate][name])
            except Exception:
                pass

        if self._dirty_count >= self._min_items_before_flush:
            if not self._pending_flush_task or self._pending_flush_task.done():
                self._pending_flush_task = asyncio.create_task(self._trigger_flush())
            self._dirty_count = 0

    async def _trigger_flush(self):
        """
        Trigger a flush with a small debounce delay.
        """
        try:
            await asyncio.sleep(0.1)
            await self.flush_once()
        except Exception:
            pass
        finally:
            self._pending_flush_task = None

    async def _atomic_write_sorted_view(self, test_copy: dict):
        """
        Write the sorted view to file atomically.
        """
        sorted_view = sort_channel_result(
            self.base_data,
            result=test_copy,
            filter_host=config.speed_test_filter_host,
            ipv6_support=self.ipv6_support,
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            write_channel_to_file,
            sorted_view,
            self.ipv6_support,
            self.first_channel_name,
            True,
            self.is_last,
        )

    async def flush_once(self, force: bool = False):
        """
        Flush the current test results to file if dirty.
        """
        async with self._lock:
            if not self._dirty and not force:
                return
            test_copy = copy.deepcopy(self.test_results)
            self._dirty = False
            self._dirty_count = 0

        try:
            await self._atomic_write_sorted_view(test_copy)
        finally:
            if self._pending_flush_task and self._pending_flush_task.done():
                self._pending_flush_task = None

    async def _run_loop(self):
        """
        Run the periodic flush loop.
        """
        self._stopped = False
        try:
            while not self._stopped:
                await asyncio.sleep(self.write_interval)
                if self._dirty:
                    try:
                        await self.flush_once()
                    except Exception:
                        pass
        finally:
            self._stopped = True

    async def start(self):
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._stopped = True
        if self._task:
            await self._task
            self._task = None
        if self.sort_logger:
            self.sort_logger.handlers.clear()
        if self.stat_logger:
            self.stat_logger.handlers.clear()
