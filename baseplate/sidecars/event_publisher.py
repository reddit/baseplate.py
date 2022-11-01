# pylint: disable=wrong-import-position,wrong-import-order
from gevent.monkey import patch_all

from baseplate.server.monkey import patch_stdlib_queues

# In order to allow Prometheus to scrape metrics, we need to concurrently
# handle requests to '/metrics' along with the sidecar's execution.
# Monkey patching is used to replace the stdlib sequential versions of functions
# with concurrent versions. It must happen as soon as possible, before the
# sequential versions are imported.
patch_all()
patch_stdlib_queues()

import argparse
import configparser
import email.utils
import gzip
import hashlib
import hmac
import logging

from typing import Any
from typing import List
from typing import Optional

import gevent
import requests

from prometheus_client import Counter

from baseplate import Baseplate
from baseplate.clients.requests import ExternalRequestsClient
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.events import MAX_EVENT_SIZE
from baseplate.lib.events import MAX_QUEUE_SIZE
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.lib.metrics import metrics_client_from_config
from baseplate.lib.retry import RetryPolicy
from baseplate.server import EnvironmentInterpolation
from baseplate.server.prometheus import start_prometheus_exporter_for_sidecar
from baseplate.sidecars import Batch
from baseplate.sidecars import BatchFull
from baseplate.sidecars import SerializedBatch
from baseplate.sidecars import TimeLimitedBatch


logger = logging.getLogger(__name__)


# seconds to wait for the event collector
POST_TIMEOUT = 3
# Base rate for expontential retry delay
RETRY_BACKOFF = 2
# Maximum wait for a message to be sent
MAX_RETRY_TIME = 5 * 60
# maximum time (seconds) an event can sit around while we wait for more
# messages to batch
MAX_BATCH_AGE = 1
# maximum size (in bytes) of a batch of events
MAX_BATCH_SIZE = 500 * 1024

PUBLISHES_COUNT_TOTAL = Counter("eventv2_publishes_total", "total count of published events")


class MaxRetriesError(Exception):
    pass


class V2Batch(Batch):
    # V2 batches are a struct with a single field of list<Event> type. because
    # we don't have the individual event schemas here, but pre-serialized
    # individual events instead, we mimic TJSONProtocol's container format
    # manually here.
    #
    # the format string in this header is for the length field. it should be
    # kept a similar number of bytes to the output of the formatting so the
    # initial size estimate is OK. 4 digits should be plenty.
    _header = '{"1":{"lst":["rec",%01d,'
    _end = b"]}}"

    def __init__(self, max_size: int = MAX_BATCH_SIZE):
        self.max_size = max_size
        self.reset()

    def add(self, item: Optional[bytes]) -> None:
        if not item:
            return

        serialized_size = len(item) + 1  # the comma at the end

        if self._size + serialized_size > self.max_size:
            raise BatchFull

        self._items.append(item)
        self._size += serialized_size

    def serialize(self) -> SerializedBatch:
        header = (self._header % len(self._items)).encode()
        return SerializedBatch(
            item_count=len(self._items), serialized=header + b",".join(self._items) + self._end
        )

    def reset(self) -> None:
        self._items: List[bytes] = []
        self._size = len(self._header) + len(self._end)


class V2JBatch(V2Batch):
    # Send a batch as a plain JSON array.  Useful when your events are not
    # Thrift JSON
    _header = "["
    _end = b"]"

    def serialize(self) -> SerializedBatch:
        serialized = self._header.encode() + b",".join(self._items) + self._end
        return SerializedBatch(item_count=len(self._items), serialized=serialized)


class BatchPublisher:
    def __init__(self, bp: Baseplate, metrics_client: metrics.Client, cfg: Any):
        self.baseplate = bp
        self.metrics = metrics_client
        self.url = f"{cfg.collector.scheme}://{cfg.collector.hostname}/v{cfg.collector.version}"
        self.key_name = cfg.key.name
        self.key_secret = cfg.key.secret

    def _sign_payload(self, payload: bytes) -> str:
        digest = hmac.new(self.key_secret, payload, hashlib.sha256).hexdigest()
        return f"key={self.key_name}, mac={digest}"

    def publish(self, payload: SerializedBatch) -> None:
        if not payload.item_count:
            return

        logger.info("sending batch of %d events", payload.item_count)
        compressed_payload = gzip.compress(payload.serialized)
        headers = {
            "Date": email.utils.formatdate(usegmt=True),
            "User-Agent": "baseplate-event-publisher/1.0",
            "Content-Type": "application/json",
            "X-Signature": self._sign_payload(payload.serialized),
            "Content-Encoding": "gzip",
        }

        for _ in RetryPolicy.new(budget=MAX_RETRY_TIME, backoff=RETRY_BACKOFF):
            try:
                with self.baseplate.server_context("post") as context:
                    with self.metrics.timer("post"):
                        response = context.http_client.post(
                            self.url,
                            headers=headers,
                            data=compressed_payload,
                            timeout=POST_TIMEOUT,
                        )
                response.raise_for_status()
            except requests.HTTPError as exc:
                self.metrics.counter("error.http").increment()

                # we should crash if it's our fault
                response = getattr(exc, "response", None)
                if response is not None and response.status_code < 500:
                    logger.exception("HTTP Request failed. Error: %s", response.text)
                    if response.status_code != 422:
                        # Do not exit on validation errors
                        raise
                else:
                    logger.exception("HTTP Request failed.")
            except OSError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                PUBLISHES_COUNT_TOTAL.inc(payload.item_count)
                self.metrics.counter("sent").increment(payload.item_count)
                return

        raise MaxRetriesError("could not sent batch")


SERIALIZER_BY_VERSION = {"2": V2Batch, "2j": V2JBatch}


def publish_events() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    arg_parser.add_argument(
        "--queue-name",
        default="main",
        help="name of event queue / publisher config (default: main)",
    )
    arg_parser.add_argument(
        "--debug", default=False, action="store_true", help="enable debug logging"
    )
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level)

    config_parser = configparser.RawConfigParser(interpolation=EnvironmentInterpolation())
    config_parser.read_file(args.config_file)
    raw_config = dict(config_parser.items("event-publisher:" + args.queue_name))
    cfg = config.parse_config(
        raw_config,
        {
            "collector": {
                "hostname": config.String,
                "version": config.Optional(config.String, default="2"),
                "scheme": config.Optional(config.String, default="https"),
            },
            "key": {"name": config.String, "secret": config.Base64},
            "max_queue_size": config.Optional(config.Integer, MAX_QUEUE_SIZE),
        },
    )

    metrics_client = metrics_client_from_config(raw_config)

    event_queue = MessageQueue(
        "/events-" + args.queue_name,
        max_messages=cfg.max_queue_size,
        max_message_size=MAX_EVENT_SIZE,
    )

    bp = Baseplate()
    bp.configure_context(
        {
            "http_client": ExternalRequestsClient("event_collector"),
        }
    )

    # pylint: disable=maybe-no-member
    serializer = SERIALIZER_BY_VERSION[cfg.collector.version]()
    batcher = TimeLimitedBatch(serializer, MAX_BATCH_AGE)
    publisher = BatchPublisher(bp, metrics_client, cfg)

    while True:
        # allow other routines to execute (specifically handling requests to /metrics)
        gevent.sleep(0)
        message: Optional[bytes]

        try:
            message = event_queue.get(timeout=0.2)
        except TimedOutError:
            message = None

        try:
            batcher.add(message)
            continue
        except BatchFull:
            pass

        serialized = batcher.serialize()
        try:
            publisher.publish(serialized)
        except Exception:
            logger.exception("Events publishing failed.")
        batcher.reset()
        batcher.add(message)


if __name__ == "__main__":
    start_prometheus_exporter_for_sidecar()
    publish_events()
