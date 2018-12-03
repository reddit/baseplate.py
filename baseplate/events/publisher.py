import argparse
import email.utils
import gzip
import hashlib
import hmac
import logging

import requests

from . import MAX_EVENT_SIZE, MAX_QUEUE_SIZE
from .. import config, metrics_client_from_config
from .. _compat import configparser, BytesIO
from .. _utils import (
    Batch, BatchFull, RawJSONBatch,
    SerializedBatch, TimeLimitedBatch,
)
from .. message_queue import MessageQueue, TimedOutError
from .. retry import RetryPolicy


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


class MaxRetriesError(Exception):
    pass


class V1Batch(RawJSONBatch):
    def __init__(self, max_size=MAX_BATCH_SIZE):
        super(V1Batch, self).__init__(max_size)


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
                    logger.exception("HTTP Request failed. Error: {}".format(response.text))
                    raise
                else:
                    logger.exception("HTTP Request failed.")
            except IOError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                self.metrics.counter("sent").increment(payload.count)
                return

        raise MaxRetriesError('could not sent batch')


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

    metrics_client = metrics_client_from_config(raw_config)

    event_queue = MessageQueue(
        "/events-" + args.queue_name,
        max_messages=MAX_QUEUE_SIZE,
        max_message_size=MAX_EVENT_SIZE,
    )

    # pylint: disable=maybe-no-member
    serializer = SERIALIZER_BY_VERSION[cfg.collector.version]()
    batcher = TimeLimitedBatch(serializer, MAX_BATCH_AGE)
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
