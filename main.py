import asyncio
import copy
import datetime
import gzip
import os
import pickle
from time import time

import pytz
from tqdm import tqdm

import utils.constants as constants
from updates.epg import get_epg
from updates.epg.tools import write_to_xml, compress_to_gz
from updates.subscribe import get_channels_by_subscribe_urls
from utils.aggregator import ResultAggregator
from utils.channel import (
    get_channel_items,
    append_total_data,
    test_speed
)
from utils.config import config
from utils.i18n import t
from utils.tools import (
    get_pbar_remaining,
    process_nested_dict,
    format_interval,
    check_ipv6_support,
    get_urls_from_file,
    get_version_info,
    get_urls_len,
    merge_objects,
    get_public_url,
    parse_times
)
from utils.types import CategoryChannelData
from utils.whitelist import load_whitelist_maps, get_section_entries


class UpdateSource:

    def __init__(self):
        self.whitelist_maps = None
        self.blacklist = None
        self.update_progress = None
        self.run_ui = False
        self.tasks = []
        self.channel_items: CategoryChannelData = {}
        self.channel_names = []
        self.subscribe_result = {}
        self.epg_result = {}
        self.channel_data: CategoryChannelData = {}
        self.pbar = None
        self.total = 0
        self.start_time = None
        self.stop_event = None
        self.ipv6_support = False
        self.now = None
        self.aggregator = None

    async def visit_page(self, channel_names: list[str] = None):
        tasks_config = [
            ("subscribe", get_channels_by_subscribe_urls, "subscribe_result"),
            ("epg", get_epg, "epg_result"),
        ]

        for setting, task_func, result_attr in tasks_config:
            if config.open_method[setting]:
                if setting == "subscribe":
                    whitelist_subscribe_urls, default_subscribe_urls = get_section_entries(constants.subscribe_path,
                                                                                           pattern=constants.url_pattern)
                    subscribe_urls = list(dict.fromkeys(whitelist_subscribe_urls + default_subscribe_urls))
                    print(t("msg.subscribe_urls_whitelist_total").format(default_count=len(default_subscribe_urls),
                                                                         whitelist_count=len(whitelist_subscribe_urls),
                                                                         total=len(subscribe_urls)))
                    if not subscribe_urls:
                        print(t("msg.no_subscribe_urls").format(file=constants.subscribe_path))
                        continue
                    task = asyncio.create_task(
                        task_func(subscribe_urls,
                                  names=channel_names,
                                  whitelist=whitelist_subscribe_urls,
                                  callback=self.update_progress
                                  )
                    )
                else:
                    task = asyncio.create_task(
                        task_func(channel_names, callback=self.update_progress)
                    )
                self.tasks.append(task)
                setattr(self, result_attr, await task)

    def pbar_update(self, name: str = "", item_name: str = ""):
        if self.pbar.n < self.total:
            self.pbar.update()
            remaining_total = self.total - self.pbar.n
            remaining_time = get_pbar_remaining(n=self.pbar.n, total=self.total, start_time=self.start_time)
            self.update_progress(
                t("msg.progress_desc").format(name=name, remaining_total=remaining_total, item_name=item_name,
                                              remaining_time=remaining_time),
                int((self.pbar.n / self.total) * 100),
            )

    async def main(self):
        try:
            main_start_time = time()
            if config.open_update:
                self.whitelist_maps = load_whitelist_maps(constants.whitelist_path)
                self.blacklist = get_urls_from_file(constants.blacklist_path, pattern_search=False)
                self.channel_items = get_channel_items(self.whitelist_maps, self.blacklist)
                self.channel_data = {}
                self.channel_names = [
                    name
                    for channel_obj in self.channel_items.values()
                    for name in channel_obj.keys()
                ]
                if not self.channel_names:
                    print(t("msg.no_channel_names").format(file=config.source_file))
                    return
                await self.visit_page(self.channel_names)
                if self.epg_result:
                    write_to_xml(self.epg_result, constants.epg_result_path)
                    compress_to_gz(constants.epg_result_path, constants.epg_gz_result_path)
                self.tasks = []
                append_total_data(
                    self.channel_items.items(),
                    self.channel_data,
                    self.subscribe_result,
                    self.whitelist_maps,
                    self.blacklist
                )
                self.aggregator = ResultAggregator(
                    base_data=self.channel_data,
                    first_channel_name=self.channel_names[0] if self.channel_names else None,
                    ipv6_support=self.ipv6_support,
                    write_interval=2.0
                )
                await self.aggregator.start()
                cache_result = self.channel_data
                if config.open_speed_test:
                    urls_total = get_urls_len(self.channel_data)
                    test_data = copy.deepcopy(self.channel_data)
                    process_nested_dict(
                        test_data,
                        seen=set(),
                        filter_host=config.speed_test_filter_host,
                        ipv6_support=self.ipv6_support
                    )
                    self.total = get_urls_len(test_data)
                    if self.total <= 0:
                        print(t("msg.total_urls_need_test_speed").format(total=urls_total, speed_total=self.total))
                        self.aggregator.is_last = True
                        await self.aggregator.flush_once(force=True)
                    else:
                        print(t("msg.total_urls_need_test_speed").format(total=urls_total, speed_total=self.total))
                        self.update_progress(
                            t("msg.progress_speed_test").format(total=urls_total, speed_total=self.total),
                            0,
                        )
                        self.start_time = time()
                        self.pbar = tqdm(total=self.total, desc=t("pbar.speed_test"))
                        test_result = await test_speed(
                            test_data,
                            ipv6=self.ipv6_support,
                            callback=lambda: self.pbar_update(name=t("pbar.speed_test"), item_name=t("pbar.url")),
                            on_task_complete=self.aggregator.add_item
                        )
                        cache_result = merge_objects(cache_result, test_result, match_key="url")
                        self.pbar.close()
                else:
                    self.aggregator.is_last = True
                    await self.aggregator.flush_once(force=True)
                await self.aggregator.stop()
                if config.open_history:
                    if os.path.exists(constants.cache_path):
                        with gzip.open(constants.cache_path, "rb") as file:
                            try:
                                cache = pickle.load(file)
                            except EOFError:
                                cache = {}
                            cache_result = merge_objects(cache, cache_result, match_key="url")
                    cache_path = constants.cache_path
                    cache_dir = os.path.dirname(cache_path)
                    if cache_dir:
                        os.makedirs(cache_dir, exist_ok=True)
                        with gzip.open(constants.cache_path, "wb") as file:
                            pickle.dump(cache_result, file)
                print(t("msg.update_completed").format(time=format_interval(time() - main_start_time), service_tip=""))
            if self.run_ui:
                open_service = config.open_service
                service_tip = t("msg.service_tip") if open_service else ""
                tip = (
                    t("msg.service_run_success").format(service_tip=service_tip)
                    if open_service and config.open_update == False
                    else t("msg.update_completed").format(time=format_interval(time() - main_start_time),
                                                          service_tip=service_tip)
                )
                self.update_progress(
                    tip,
                    100,
                    finished=True,
                    url=f"{get_public_url()}" if open_service else None,
                    now=self.now
                )
        except asyncio.exceptions.CancelledError:
            print(t("msg.update_cancelled"))

    async def start(self, callback=None):
        def default_callback(self, *args, **kwargs):
            pass

        self.update_progress = callback or default_callback
        self.run_ui = True if callback else False
        if self.run_ui:
            self.update_progress(t("msg.check_ipv6_support"), 0)
        self.ipv6_support = config.ipv6_support or check_ipv6_support()
        if not os.getenv("GITHUB_ACTIONS") and (config.update_interval or config.update_times):
            await self.scheduler(asyncio.Event())
        elif config.update_startup:
            await self.main()

    def stop(self):
        for task in self.tasks:
            task.cancel()
        self.tasks = []
        if self.pbar:
            self.pbar.close()
        if self.stop_event:
            self.stop_event.set()

    async def scheduler(self, stop_event):
        self.stop_event = stop_event
        tz = pytz.timezone(config.time_zone)
        mode = config.update_mode
        update_times = parse_times(config.update_times)

        try:
            self.now = datetime.datetime.now(tz)
            if config.update_startup:
                await self.main()

            while not stop_event.is_set():
                self.now = datetime.datetime.now(tz)

                if mode == "time" and update_times:
                    candidates = []
                    for h, m in update_times:
                        candidate = self.now.replace(hour=h, minute=m, second=0, microsecond=0)
                        if candidate <= self.now:
                            candidate = candidate + datetime.timedelta(days=1)
                        candidates.append(candidate)
                    next_time = min(candidates)
                    wait_seconds = (next_time - self.now).total_seconds()
                    print(t("msg.schedule_update_time").format(time=next_time.strftime("%Y-%m-%d %H:%M:%S")))
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                        if stop_event.is_set():
                            break
                    except asyncio.TimeoutError:
                        self.now = datetime.datetime.now(tz)
                        await self.main()
                        continue
                else:
                    next_time = self.now + datetime.timedelta(hours=config.update_interval)
                    print(t("msg.schedule_update_time").format(time=next_time.strftime("%Y-%m-%d %H:%M:%S")))
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=config.update_interval * 3600)
                    except asyncio.TimeoutError:
                        continue
        except asyncio.CancelledError:
            print(t("msg.schedule_cancelled"))


if __name__ == "__main__":
    info = get_version_info()
    print(t("msg.version_info").format(name=info['name'], version=info['version']))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    update_source = UpdateSource()
    loop.run_until_complete(update_source.start())
