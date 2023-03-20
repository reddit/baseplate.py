import argparse
import configparser
import email.utils
import gzip
import hashlib
import hmac
import logging
import signal
import sys

from types import FrameType
from typing import Any
from typing import List
from typing import Optional

import requests

from baseplate import __version__ as baseplate_version
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.events import MAX_EVENT_SIZE
from baseplate.lib.events import MAX_QUEUE_SIZE
from baseplate.lib.message_queue import create_queue
from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import QueueType
from baseplate.lib.message_queue import TimedOutError
from baseplate.lib.metrics import metrics_client_from_config
from baseplate.lib.retry import RetryPolicy
from baseplate.server import EnvironmentInterpolation
from baseplate.sidecars import Batch
from baseplate.sidecars import BatchFull
from baseplate.sidecars import publisher_queue_utils
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
# Seconds to wait for get/put operations on the event queue
QUEUE_TIMEOUT = 0.2
# Default address for remote queue server
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9091


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
    def __init__(self, metrics_client: metrics.Client, cfg: Any):
        self.metrics = metrics_client
        self.url = f"{cfg.collector.scheme}://{cfg.collector.hostname}/v{cfg.collector.version}"
        self.key_name = cfg.key.name
        self.key_secret = cfg.key.secret
        self.session = requests.Session()
        self.session.headers[
            "User-Agent"
        ] = f"baseplate.py-{self.__class__.__name__}/{baseplate_version}"

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
                with self.metrics.timer("post"):
                    response = self.session.post(
                        self.url,
                        headers=headers,
                        data=compressed_payload,
                        timeout=POST_TIMEOUT,
                        # http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
                        stream=False,
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
                self.metrics.counter("sent").increment(payload.item_count)
                return

        raise MaxRetriesError("could not sent batch")


SERIALIZER_BY_VERSION = {"2": V2Batch, "2j": V2JBatch}


def serialize_and_publish_batch(publisher: BatchPublisher, batcher: TimeLimitedBatch) -> None:
    """Serializes batch, publishes it using the publisher, and then resets the batch for more messages."""
    serialized_batch = batcher.serialize()
    try:
        publisher.publish(serialized_batch)
    except Exception:
        logger.exception("Events publishing failed.")
    batcher.reset()


def build_and_publish_batch(
    event_queue: MessageQueue, batcher: TimeLimitedBatch, publisher: BatchPublisher, timeout: float
) -> bytes:
    """Continuously polls for messages, then batches and publishes them."""
    while True:
        message: Optional[bytes]
        try:
            message = event_queue.get(timeout)
            batcher.add(message)
            continue  # Start loop again - we will publish on the next loop if batch full/queue empty
        except TimedOutError:
            message = None
            # Keep going - we may want to publish if we have other messages in the batch and time is up
        except BatchFull:
            batcher.is_full = True
            # Keep going - we want to publish bc batch is full

        if batcher.is_ready:  # Time is up or batch is full
            serialize_and_publish_batch(publisher, batcher)

            if (
                message
            ):  # If we published because batch was full, we need to add the straggler we popped
                batcher.add(message)


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
            "max_element_size": config.Optional(config.Integer, MAX_EVENT_SIZE),
            "queue_type": config.Optional(config.String, default=QueueType.POSIX.value),
            "queue_host": config.Optional(config.String, DEFAULT_HOST),
            "queue_port": config.Optional(config.Integer, DEFAULT_PORT),
        },
    )

    metrics_client = metrics_client_from_config(raw_config)

    queue_name = f"/events-{args.queue_name}"
    event_queue: MessageQueue = create_queue(
        cfg.queue_type, queue_name, cfg.max_queue_size, cfg.max_element_size
    )

    # pylint: disable=maybe-no-member
    serializer = SERIALIZER_BY_VERSION[cfg.collector.version]()
    batcher = TimeLimitedBatch(serializer, MAX_BATCH_AGE)
    publisher = BatchPublisher(metrics_client, cfg)

    def flush_queue_signal_handler(_signo: int, _frame: FrameType) -> None:
        """Signal handler for flushing messages from the queue and publishing them."""
        message: Optional[bytes]
        logger.info("Shutdown signal received. Flushing events...")

        while True:
            try:
                message = event_queue.get(timeout=0.2)
            except TimedOutError:
                # Once the queue drains, publish anything remaining and then exit
                if len(batcher.serialize()) > 0:
                    serialize_and_publish_batch(publisher, batcher)
                break

            if batcher.is_ready:
                serialize_and_publish_batch(publisher, batcher)
            batcher.add(message)
        sys.exit(0)

    if cfg.queue_type == QueueType.IN_MEMORY.value and isinstance(
        event_queue, InMemoryMessageQueue
    ):
        # Start the Thrift server that communicates with RemoteMessageQueues and stores
        # data in a InMemoryMessageQueue
        with publisher_queue_utils.start_queue_server(
            event_queue, host=cfg.queue_host, port=cfg.queue_port
        ):
            # Note: shutting down gracefully with gevent is complicated, so we are not
            # implementing for now. There is the possibility of event loss on shutdown.
            build_and_publish_batch(event_queue, batcher, publisher, QUEUE_TIMEOUT)

    else:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, flush_queue_signal_handler)
            signal.siginterrupt(sig, False)
        build_and_publish_batch(event_queue, batcher, publisher, QUEUE_TIMEOUT)


if __name__ == "__main__":
    publish_events()
