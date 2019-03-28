from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import socket
import threading
import time

from baseplate import config


REPORT_INTERVAL_SECONDS = 10


class _ConcurrencyReporter(object):
    def __init__(self, pool):
        self.pool = pool

    def report(self, batch):
        batch.gauge("active_requests").replace(len(self.pool.greenlets))


class _BlockedGeventHubReporter(object):
    def __init__(self, max_blocking_time):
        try:
            import gevent.events
        except ImportError:
            raise Exception("Gevent >=1.3 required")

        gevent.events.subscribers.append(self._on_gevent_event)
        gevent.config.monitor_thread = True
        gevent.config.max_blocking_time = max_blocking_time
        gevent.get_hub().start_periodic_monitoring_thread()

        self.times_blocked = []

    def _on_gevent_event(self, event):
        import gevent.events

        if isinstance(event, gevent.events.EventLoopBlocked):
            self.times_blocked.append(event.blocking_time)

    def report(self, batch):
        # gevent events come in on another thread. we're relying on the GIL to
        # keep us from shenanigans here and we swap things out semi-safely to
        # ensure minimal lost data.
        times_blocked = self.times_blocked
        self.times_blocked = []

        for time_blocked in times_blocked:
            batch.timer("hub_blocked").send(time_blocked)


def _report_runtime_metrics_periodically(metrics_client, reporters):
    hostname = socket.gethostname()
    pid = os.getpid()

    while True:
        now = time.time()
        time_since_last_report = now % REPORT_INTERVAL_SECONDS
        time_until_next_report = REPORT_INTERVAL_SECONDS - time_since_last_report
        time.sleep(time_until_next_report)

        try:
            with metrics_client.batch() as batch:
                batch.namespace += ".runtime.{}.PID{}".format(hostname, pid).encode()
                for reporter in reporters:
                    try:
                        reporter.report(batch)
                    except Exception as exc:
                        logging.debug("Error generating server metrics: %s: %s",
                                      reporter.__class__.__name__, exc)
        except Exception as exc:
            logging.debug("Error while sending server metrics: %s", exc)


def start(server_config, application, pool):
    if not hasattr(application, "baseplate") or not application.baseplate._metrics_client:
        logging.info("No metrics client configured. Server metrics will not be sent.")
        return

    cfg = config.parse_config(server_config, {
        "monitoring": {
            "blocked_hub": config.Optional(config.Timespan, default=None),
            "concurrency": config.Optional(config.Boolean, default=True),
        },
    })

    reporters = []

    if cfg.monitoring.concurrency:
        reporters.append(_ConcurrencyReporter(pool))

    if cfg.monitoring.blocked_hub is not None:
        try:
            reporters.append(_BlockedGeventHubReporter(cfg.monitoring.blocked_hub.total_seconds()))
        except Exception as exc:
            logging.info("monitoring.blocked_hub disabled: %s", exc)

    thread = threading.Thread(
        name="Server Monitoring",
        target=_report_runtime_metrics_periodically,
        args=(application.baseplate._metrics_client, reporters),
    )
    thread.daemon = True
    thread.start()
