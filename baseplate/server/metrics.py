from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import socket
import time

import gevent


REPORT_INTERVAL_SECONDS = 10


def _report_runtime_metrics(metrics_client, hostname, pid, pool):
    try:
        with metrics_client.batch() as batch:
            active_requests = batch.gauge("runtime.%s.PID%s.active_requests" % (hostname, pid))
            active_requests.replace(len(pool.greenlets))
    except Exception as exc:
        logging.debug("Error while generating server metrics: %s", exc)


def _report_runtime_metrics_periodically(metrics_client, hostname, pid, pool):
    while True:
        now = time.time()
        time_since_last_report = now % REPORT_INTERVAL_SECONDS
        time_until_next_report = REPORT_INTERVAL_SECONDS - time_since_last_report
        time.sleep(time_until_next_report)

        _report_runtime_metrics(metrics_client, hostname, pid, pool)


def start_runtime_metrics_reporter(application, pool):
    metrics_client = application.baseplate._metrics_client
    if not metrics_client:
        logging.info("No metrics client configured. Server metrics will not be sent.")
        return

    hostname = socket.gethostname()
    pid = os.getpid()

    gevent.spawn(
        _report_runtime_metrics_periodically,
        metrics_client,
        hostname,
        pid,
        pool,
    )
