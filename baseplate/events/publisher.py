import argparse
import collections
import email.utils
import gzip
import hashlib
import hmac
import logging
import time

import requests

from . import MAX_EVENT_SIZE, MAX_QUEUE_SIZE
from .. import config, make_metrics_client
from .. _compat import configparser, BytesIO
from .. message_queue import MessageQueue, TimedOutError


logger = logging.getLogger(__name__)


# seconds to wait for the event collector
POST_TIMEOUT = 3
# maximum time (seconds) an event can sit around while we wait for more
# messages to batch
MAX_BATCH_AGE = 1
# maximum size (in bytes) of a batch of events
MAX_BATCH_SIZE = 500 * 1024
# seconds to wait between retries when the collector fails
RETRY_DELAY = 2


SerializedBatch = collections.namedtuple("SerializedBatch", "count bytes")


class BatchFull(Exception):
    pass


class Batch(object):
    def add(self, item):
        raise NotImplementedError

    def serialize(self):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError


class TimeLimitedBatch(Batch):
    def __init__(self, inner, max_age=MAX_BATCH_AGE):
        self.batch = inner
        self.batch_start = None
        self.max_age = max_age

    @property
    def age(self):
        if not self.batch_start:
            return 0
        return time.time() - self.batch_start

    def add(self, item):
        if self.age >= self.max_age:
            raise BatchFull

        self.batch.add(item)

        if not self.batch_start:
            self.batch_start = time.time()

    def serialize(self):
        return self.batch.serialize()

    def reset(self):
        self.batch.reset()
        self.batch_start = None


class V1Batch(Batch):
    def __init__(self, max_size=MAX_BATCH_SIZE):
        self.max_size = max_size
        self.reset()

    def add(self, item):
        if not item:
            return

        serialized_size = len(item) + 1  # the comma at the end

        if self._size + serialized_size > self.max_size:
            raise BatchFull

        self._items.append(item)
        self._size += serialized_size

    def serialize(self):
        return SerializedBatch(
            count=len(self._items),
            bytes=b"[" + b",".join(self._items) + b"]",
        )

    def reset(self):
        self._items = []
        self._size = 2  # the [] that wrap the json list


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
    _end = b']}}'

    def __init__(self, max_size=MAX_BATCH_SIZE):
        self.max_size = max_size
        self.reset()

    def add(self, item):
        if not item:
            return

        serialized_size = len(item) + 1  # the comma at the end

        if self._size + serialized_size > self.max_size:
            raise BatchFull

        self._items.append(item)
        self._size += serialized_size

    def serialize(self):
        header = (self._header % len(self._items)).encode()
        return SerializedBatch(
            count=len(self._items),
            bytes=header + b",".join(self._items) + self._end,
        )

    def reset(self):
        self._items = []
        self._size = len(self._header) + len(self._end)


def gzip_compress(content):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=9) as gzip_file:
        gzip_file.write(content)
    return buf.getvalue()


class BatchPublisher(object):
    def __init__(self, metrics_client, cfg):
        self.metrics = metrics_client
        self.url = "https://%s/v%d" % (cfg.collector.hostname, cfg.collector.version)
        self.key_name = cfg.key.name
        self.key_secret = cfg.key.secret
        self.session = requests.Session()

    def _sign_payload(self, payload):
        digest = hmac.new(self.key_secret, payload, hashlib.sha256).hexdigest()
        return "key={key}, mac={mac}".format(key=self.key_name, mac=digest)

    def publish(self, payload):
        if not payload.count:
            return

        logger.info("sending batch of %d events", payload.count)
        compressed_payload = gzip_compress(payload.bytes)
        headers = {
            "Date": email.utils.formatdate(usegmt=True),
            "User-Agent": "baseplate-event-publisher/1.0",
            "Content-Type": "application/json",
            "X-Signature": self._sign_payload(payload.bytes),
            "Content-Encoding": "gzip",
        }

        while True:
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
                logger.exception("HTTP Request failed")

                # we should crash if it's our fault
                response = getattr(exc, "response", None)
                if response is not None and response.status_code < 500:
                    raise
            except IOError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                self.metrics.counter("sent").increment(payload.count)
                return

            time.sleep(RETRY_DELAY)


SERIALIZER_BY_VERSION = {
    1: V1Batch,
    2: V2Batch,
}


def publish_events():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    arg_parser.add_argument("--queue-name", default="main",
        help="name of event queue / publisher config (default: main)")
    arg_parser.add_argument("--debug", default=False, action="store_true",
        help="enable debug logging")
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level)

    config_parser = configparser.RawConfigParser()
    config_parser.readfp(args.config_file)
    raw_config = dict(config_parser.items("event-publisher:" + args.queue_name))
    cfg = config.parse_config(raw_config, {
        "collector": {
            "hostname": config.String,
            "version": config.Optional(config.Integer, default=1),
        },

        "key": {
            "name": config.String,
            "secret": config.Base64,
        },
    })

    metrics_client = make_metrics_client(raw_config)

    event_queue = MessageQueue(
        "/events-" + args.queue_name,
        max_messages=MAX_QUEUE_SIZE,
        max_message_size=MAX_EVENT_SIZE,
    )

    # pylint: disable=maybe-no-member
    serializer = SERIALIZER_BY_VERSION[cfg.collector.version]()
    batcher = TimeLimitedBatch(serializer)
    publisher = BatchPublisher(metrics_client, cfg)

    while True:
        try:
            message = event_queue.get(timeout=.2)
        except TimedOutError:
            message = None

        try:
            batcher.add(message)
        except BatchFull:
            serialized = batcher.serialize()
            publisher.publish(serialized)
            batcher.reset()
            batcher.add(message)


if __name__ == "__main__":
    publish_events()
