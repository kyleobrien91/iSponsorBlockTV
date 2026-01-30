import asyncio
import logging
import time
import os
from signal import SIGINT, SIGTERM, signal
from typing import Optional

import aiohttp
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from . import api_helpers, ytlounge
from .debug_helpers import AiohttpTracer


class DeviceListener:
    def __init__(self, api_helper, config, device, debug: bool, web_session):
        self.task: Optional[asyncio.Task] = None
        self.api_helper = api_helper
        self.offset = device.offset
        self.name = device.name
        self.cancelled = False
        self.logger = logging.getLogger(f"iSponsorBlockTV-{device.screen_id}")
        self.web_session = web_session
        self.lounge_controller = ytlounge.YtLoungeApi(
            device.screen_id, config, api_helper, self.logger
        )

    def update_config(self, config, device):
        self.offset = device.offset
        self.name = device.name
        self.lounge_controller.update_config(config)

    # Ensures that we have a valid auth token
    async def refresh_auth_loop(self):
        while True:
            await asyncio.sleep(60 * 60 * 24)  # Refresh every 24 hours
            try:
                await self.lounge_controller.refresh_auth()
            except BaseException:
                pass

    async def is_available(self):
        try:
            return await self.lounge_controller.is_available()
        except BaseException:
            return False

    # Main subscription loop
    async def loop(self):
        lounge_controller = self.lounge_controller
        while not self.cancelled:
            while not lounge_controller.linked():
                try:
                    self.logger.debug("Refreshing auth")
                    await lounge_controller.refresh_auth()
                except BaseException:
                    await asyncio.sleep(10)
            while not (await self.is_available()) and not self.cancelled:
                self.logger.debug("Waiting for device to be available")
                await asyncio.sleep(10)
            try:
                await lounge_controller.connect()
            except BaseException:
                pass
            while not lounge_controller.connected() and not self.cancelled:
                # Doesn't connect to the device if it's a kids profile (it's broken)
                self.logger.debug("Waiting for device to be connected")
                await asyncio.sleep(10)
                try:
                    await lounge_controller.connect()
                except BaseException:
                    pass
            self.logger.info(
                "Connected to device %s (%s)", lounge_controller.screen_name, self.name
            )
            try:
                self.logger.debug("Subscribing to lounge")
                sub = await lounge_controller.subscribe_monitored(self)
                await sub
            except BaseException:
                pass

    # Method called on playback state change
    async def __call__(self, state):
        time_start = time.monotonic()
        try:
            self.task.cancel()
        except BaseException:
            pass
        self.task = asyncio.create_task(self.process_playstatus(state, time_start))

    # Processes the playback state change
    async def process_playstatus(self, state, time_start):
        segments = []
        if state.videoId:
            segments = await self.api_helper.get_segments(state.videoId)
        if state.state.value == 1:  # Playing
            self.logger.info("Playing video %s with %d segments", state.videoId, len(segments))
            if segments:  # If there are segments
                await self.time_to_segment(segments, state.currentTime, time_start)

    # Finds the next segment to skip to and skips to it
    async def time_to_segment(self, segments, position, time_start):
        start_next_segment = None
        next_segment = None
        for segment in segments:
            segment_start = segment["start"]
            segment_end = segment["end"]
            is_within_start_range = (
                position < 1 < segment_end and segment_start <= position < segment_end
            )
            is_beyond_current_position = segment_start > position

            if is_within_start_range or is_beyond_current_position:
                next_segment = segment
                start_next_segment = position if is_within_start_range else segment_start
                break
        if start_next_segment:
            time_to_next = (
                (start_next_segment - position - (time.monotonic() - time_start))
                / self.lounge_controller.playback_speed
            ) - self.offset
            await self.skip(time_to_next, next_segment["end"], next_segment["UUID"])

    # Skips to the next segment (waits for the time to pass)
    async def skip(self, time_to, position, uuids):
        await asyncio.sleep(time_to)
        self.logger.info("Skipping segment: seeking to %s", position)
        await asyncio.gather(
            asyncio.create_task(self.lounge_controller.seek_to(position)),
            asyncio.create_task(self.api_helper.mark_viewed_segments(uuids)),
        )

    async def cancel(self):
        self.cancelled = True
        await self.lounge_controller.disconnect()
        if self.task:
            self.task.cancel()
        if self.lounge_controller.subscribe_task_watchdog:
            self.lounge_controller.subscribe_task_watchdog.cancel()
        if self.lounge_controller.subscribe_task:
            self.lounge_controller.subscribe_task.cancel()
        await asyncio.gather(
            self.task,
            self.lounge_controller.subscribe_task_watchdog,
            self.lounge_controller.subscribe_task,
            return_exceptions=True,
        )

    async def initialize_web_session(self):
        await self.lounge_controller.change_web_session(self.web_session)


async def finish(devices_map, tasks_map, web_session, tcp_connector, observer):
    if observer:
        observer.stop()
        observer.join()

    cancellation_tasks = []
    for device in devices_map.values():
        cancellation_tasks.append(device.cancel())

    for tasks in tasks_map.values():
        for task in tasks:
            task.cancel()
            cancellation_tasks.append(task)

    await asyncio.gather(*cancellation_tasks, return_exceptions=True)
    await web_session.close()
    await tcp_connector.close()


def handle_signal(signum, frame):
    raise KeyboardInterrupt()


class ConfigHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if os.path.basename(event.src_path) == "config.json":
            self.callback()


async def supervisor(
    config, api_helper, devices_map, tasks_map, change_event, debug, web_session
):
    loop = asyncio.get_running_loop()
    while True:
        await change_event.wait()
        change_event.clear()

        # Debounce: wait a bit to see if more events come in
        await asyncio.sleep(0.5)
        # Clear any events that happened during the sleep
        change_event.clear()

        if not config.reload():
            continue

        logging.info("Configuration changed, reloading...")
        api_helper.update_config(config)

        current_screen_ids = set()

        # Update or Add
        for device_config in config.devices:
            screen_id = device_config.screen_id
            current_screen_ids.add(screen_id)

            if screen_id in devices_map:
                # Update
                logging.info(f"Updating device {screen_id}")
                devices_map[screen_id].update_config(config, device_config)
            else:
                # Add
                logging.info(f"Adding new device {screen_id}")
                try:
                    device_listener = DeviceListener(
                        api_helper, config, device_config, debug, web_session
                    )
                    await device_listener.initialize_web_session()

                    t1 = loop.create_task(device_listener.loop())
                    t2 = loop.create_task(device_listener.refresh_auth_loop())

                    devices_map[screen_id] = device_listener
                    tasks_map[screen_id] = [t1, t2]
                except Exception as e:
                    logging.error(f"Failed to initialize device {screen_id}: {e}")

        # Remove
        for screen_id in list(devices_map.keys()):
            if screen_id not in current_screen_ids:
                logging.info(f"Removing device {screen_id}")
                await devices_map[screen_id].cancel()
                for task in tasks_map[screen_id]:
                    task.cancel()
                del devices_map[screen_id]
                del tasks_map[screen_id]


async def main_async(config, debug, http_tracing):
    loop = asyncio.get_event_loop_policy().get_event_loop()
    devices_map = {}  # screen_id -> DeviceListener
    tasks_map = {}  # screen_id -> [tasks]

    if debug:
        loop.set_debug(True)

    tcp_connector = aiohttp.TCPConnector(ttl_dns_cache=300)

    # Configure session with tracing if enabled
    if http_tracing:
        root_logger = logging.getLogger("aiohttp_trace")
        tracer = AiohttpTracer(root_logger)
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(tracer.on_request_start)
        trace_config.on_response_chunk_received.append(tracer.on_response_chunk_received)
        trace_config.on_request_end.append(tracer.on_request_end)
        trace_config.on_request_exception.append(tracer.on_request_exception)
        web_session = aiohttp.ClientSession(
            trust_env=config.use_proxy,
            connector=tcp_connector,
            trace_configs=[trace_config],
        )
    else:
        web_session = aiohttp.ClientSession(
            trust_env=config.use_proxy, connector=tcp_connector
        )

    api_helper = api_helpers.ApiHelper(config, web_session)

    for i in config.devices:
        device = DeviceListener(api_helper, config, i, debug, web_session)
        devices_map[i.screen_id] = device
        await device.initialize_web_session()
        t1 = loop.create_task(device.loop())
        t2 = loop.create_task(device.refresh_auth_loop())
        tasks_map[i.screen_id] = [t1, t2]

    # Setup Watchdog
    change_event = asyncio.Event()

    def on_change():
        loop.call_soon_threadsafe(change_event.set)

    event_handler = ConfigHandler(on_change)
    observer = Observer()
    config_dir = os.path.dirname(config.config_file) or "."
    observer.schedule(event_handler, path=config_dir, recursive=False)
    observer.start()

    supervisor_task = loop.create_task(
        supervisor(
            config, api_helper, devices_map, tasks_map, change_event, debug, web_session
        )
    )

    signal(SIGTERM, handle_signal)
    signal(SIGINT, handle_signal)

    try:
        # Wait indefinitely (or until cancelled)
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("Cancelling tasks and exiting...")
        supervisor_task.cancel()
        await finish(devices_map, tasks_map, web_session, tcp_connector, observer)
    finally:
        # Ensure cleanup if not done
        if not web_session.closed:
            await web_session.close()
        if not tcp_connector.closed:
            await tcp_connector.close()
        if observer.is_alive():
            observer.stop()
            observer.join()
        print("Exited")


def main(config, debug, http_tracing):
    asyncio.run(main_async(config, debug, http_tracing))
