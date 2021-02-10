import gc
import logging
import os
import socket
import threading
import time

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NoReturn
from typing import Optional

import gevent.events

from gevent.pool import Pool

from baseplate import _ExcInfo
from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate import ServerSpanObserver
from baseplate.lib import config
from baseplate.lib import metrics


REPORT_INTERVAL_SECONDS = 10
MAX_REQUEST_AGE = 60


logger = logging.getLogger(__name__)


class _Reporter:
    def report(self, batch: metrics.Batch) -> None:
        raise NotImplementedError


class _OpenConnectionsReporter(_Reporter):
    def __init__(self, pool: Pool):
        self.pool = pool

    def report(self, batch: metrics.Batch) -> None:
        batch.gauge("open_connections").replace(len(self.pool.greenlets))


class _ActiveRequestsObserver(BaseplateObserver, _Reporter):
    def __init__(self) -> None:
        self.live_requests: Dict[str, float] = {}

    def on_server_span_created(self, context: RequestContext, server_span: ServerSpan) -> None:
        observer = _ActiveRequestsServerSpanObserver(self, server_span.trace_id)
        server_span.register(observer)

    def report(self, batch: metrics.Batch) -> None:
        threshold = time.time() - MAX_REQUEST_AGE
        stale_requests = [
            trace_id
            for trace_id, start_time in self.live_requests.items()
            if start_time < threshold
        ]
        for stale_request_id in stale_requests:
            self.live_requests.pop(stale_request_id, None)

        batch.gauge("active_requests").replace(len(self.live_requests))


class _ActiveRequestsServerSpanObserver(ServerSpanObserver):
    def __init__(self, reporter: _ActiveRequestsObserver, trace_id: str):
        self.reporter = reporter
        self.trace_id = trace_id

    def on_start(self) -> None:
        self.reporter.live_requests[self.trace_id] = time.time()

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.reporter.live_requests.pop(self.trace_id, None)


class _BlockedGeventHubReporter(_Reporter):
    def __init__(self, max_blocking_time: int):
        gevent.events.subscribers.append(self._on_gevent_event)
        gevent.config.monitor_thread = True
        gevent.config.max_blocking_time = max_blocking_time
        gevent.get_hub().start_periodic_monitoring_thread()

        self.times_blocked: List[int] = []

    def _on_gevent_event(self, event: Any) -> None:
        if isinstance(event, gevent.events.EventLoopBlocked):
            self.times_blocked.append(event.blocking_time)

    def report(self, batch: metrics.Batch) -> None:
        # gevent events come in on another thread. we're relying on the GIL to
        # keep us from shenanigans here and we swap things out semi-safely to
        # ensure minimal lost data.
        times_blocked = self.times_blocked
        self.times_blocked = []

        for time_blocked in times_blocked:
            batch.timer("hub_blocked").send(time_blocked)


class _GCStatsReporter(_Reporter):
    def report(self, batch: metrics.Batch) -> None:
        for generation, stats in enumerate(gc.get_stats()):
            for name, value in stats.items():
                gauge = batch.gauge(f"gc.{name}", tags={"generation": generation})
                gauge.replace(value)


class _GCTimingReporter(_Reporter):
    def __init__(self) -> None:
        gc.callbacks.append(self._on_gc_event)

        self.gc_durations: List[float] = []
        self.current_gc_start: Optional[float] = None

    def _on_gc_event(self, phase: str, _info: Dict[str, Any]) -> None:
        if phase == "start":
            self.current_gc_start = time.time()
        elif phase == "stop":
            if self.current_gc_start:
                elapsed = time.time() - self.current_gc_start
                self.current_gc_start = None
                self.gc_durations.append(elapsed)

    def report(self, batch: metrics.Batch) -> None:
        gc_durations = self.gc_durations
        self.gc_durations = []

        for gc_duration in gc_durations:
            batch.timer("gc.elapsed").send(gc_duration)


class _BaseplateReporter(_Reporter):
    def __init__(self, reporters: Dict[str, Callable[[Any], None]]):
        self.reporters = reporters

    def report(self, batch: metrics.Batch) -> None:
        for name, reporter in self.reporters.items():
            try:
                batch.base_tags["client"] = name
                reporter(batch)
            except Exception as exc:
                logger.exception("Error generating client metrics: %s: %s", name, exc)
            finally:
                del batch.base_tags["client"]


class _RefCycleReporter(_Reporter):
    def __init__(self, root: str):
        assert os.path.isdir(root), f"{root} is not a directory"
        self.root = root

        # test that this is available up front
        import objgraph

        del objgraph

        logger.warning("Disabling automatic garbage collection to watch for reference cycles.")
        gc.disable()

    def report(self, batch: metrics.Batch) -> None:  # pylint: disable=unused-argument
        # run a garbage collection but keep everything we found in gc.garbage
        gc.set_debug(gc.DEBUG_SAVEALL)
        gc.collect()

        if gc.garbage:
            import objgraph

            logger.warning("%d objects garbage collected. Writing objgraph...", len(gc.garbage))
            objgraph.show_backrefs(
                gc.garbage, filename=f"{self.root}/backrefs-{int(time.time())}.png"
            )

            # clean out the garbage altogether
            gc.garbage.clear()
            gc.set_debug(0)
            gc.collect()
        else:
            logger.debug("No garbage yet!")


def _report_runtime_metrics_periodically(
    metrics_client: metrics.Client, reporters: List[_Reporter]
) -> NoReturn:
    hostname = socket.gethostname()
    pid = str(os.getpid())

    while True:
        now = time.time()
        time_since_last_report = now % REPORT_INTERVAL_SECONDS
        time_until_next_report = REPORT_INTERVAL_SECONDS - time_since_last_report
        time.sleep(time_until_next_report)

        try:
            with metrics_client.batch() as batch:
                batch.namespace += b".runtime"
                batch.base_tags["hostname"] = hostname
                batch.base_tags["PID"] = pid

                for reporter in reporters:
                    try:
                        reporter.report(batch)
                    except Exception as exc:
                        logger.debug(
                            "Error generating server metrics: %s: %s",
                            reporter.__class__.__name__,
                            exc,
                        )
        except Exception as exc:
            logger.debug("Error while sending server metrics: %s", exc)


def start(server_config: Dict[str, str], application: Any, pool: Pool) -> None:
    baseplate: Baseplate = getattr(application, "baseplate", None)
    if not baseplate or not baseplate._metrics_client:
        logger.info("No metrics client configured. Server metrics will not be sent.")
        return

    cfg = config.parse_config(
        server_config,
        {
            "monitoring": {
                "blocked_hub": config.Optional(config.Timespan, default=None),
                "concurrency": config.Optional(config.Boolean, default=True),
                "connection_pool": config.Optional(config.Boolean, default=False),
                "gc": {
                    "stats": config.Optional(config.Boolean, default=True),
                    "timing": config.Optional(config.Boolean, default=False),
                    "refcycle": config.Optional(config.String, default=None),
                },
            }
        },
    )

    reporters: List[_Reporter] = []

    if cfg.monitoring.concurrency:
        reporters.append(_OpenConnectionsReporter(pool))
        observer = _ActiveRequestsObserver()
        reporters.append(observer)
        baseplate.register(observer)

    if cfg.monitoring.connection_pool:
        reporters.append(_BaseplateReporter(baseplate.get_runtime_metric_reporters()))

    if cfg.monitoring.blocked_hub is not None:
        try:
            reporters.append(_BlockedGeventHubReporter(cfg.monitoring.blocked_hub.total_seconds()))
        except Exception as exc:
            logger.info("monitoring.blocked_hub disabled: %s", exc)

    if cfg.monitoring.gc.stats:
        try:
            reporters.append(_GCStatsReporter())
        except Exception as exc:
            logger.info("monitoring.gc.stats disabled: %s", exc)

    if cfg.monitoring.gc.timing:
        try:
            reporters.append(_GCTimingReporter())
        except Exception as exc:
            logger.info("monitoring.gc.timing disabled: %s", exc)

    if cfg.monitoring.gc.refcycle:
        try:
            reporters.append(_RefCycleReporter(cfg.monitoring.gc.refcycle))
        except Exception as exc:
            logger.info("monitoring.gc.refcycle disabled: %s", exc)

    thread = threading.Thread(
        name="Server Monitoring",
        target=_report_runtime_metrics_periodically,
        args=(application.baseplate._metrics_client, reporters),
    )
    thread.daemon = True
    thread.start()
