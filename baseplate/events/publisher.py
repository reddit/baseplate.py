import argparse
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
# seconds to wait between retries when the collector fails
RETRY_DELAY = 2


class Batcher(object):
    """Time-aware batching producer.

    A batch-consumer gives necessary context to this object. It must provide:
        * batch_size_limit - the maximum size of an individual batch
        * batch_size_overhead - base size cost of a batch
        * get_item_size() - given an item, return its batch-relevant size
        * consume_batch() - consume a fully formed batch

    A flush may occur if the batch reaches the maximum specified size during
    add() or if explicitly flush()ed.

    """
    def __init__(self, consumer):
        self.consumer = consumer
        self.batch = []
        self.batch_size = self.consumer.batch_size_overhead
        self.batch_start = None

    @property
    def batch_age(self):
        """Return the age in seconds of the oldest item in the batch.

        If there are no items in the batch, 0 is returned.

        """
        if not self.batch_start:
            return 0
        return time.time() - self.batch_start

    def add(self, item):
        """Add an item to the batch, potentially flushing."""
        item_size = self.consumer.get_item_size(item)
        if self.batch_size + item_size > self.consumer.batch_size_limit:
            self.flush()
        self.batch.append(item)
        self.batch_size += item_size
        if not self.batch_start:
            self.batch_start = time.time()

    def flush(self):
        """Explicitly flush the batch if any items are enqueued."""
        if self.batch:
            self.consumer.consume_batch(self.batch)
            self.batch = []
            self.batch_size = self.consumer.batch_size_overhead
            self.batch_start = None


def gzip_compress(content):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=9) as gzip_file:
        gzip_file.write(content)
    return buf.getvalue()


class BatchConsumer(object):
    batch_size_overhead = 2  # the [] that wrap the json list
    batch_size_limit = 500 * 1024

    def __init__(self, metrics_client, cfg):
        self.metrics = metrics_client
        self.url = "https://%s/v1" % cfg.collector.hostname
        self.key_name = cfg.key.name
        self.key_secret = cfg.key.secret
        self.session = requests.Session()

    @staticmethod
    def get_item_size(item):
        # the item is already serialized, so we'll just add one byte for comma
        return len(item) + 1

    def _sign_payload(self, payload):
        digest = hmac.new(self.key_secret, payload, hashlib.sha256).hexdigest()
        return "key={key}, mac={mac}".format(key=self.key_name, mac=digest)

    def consume_batch(self, items):
        logger.info("sending batch of %d items", len(items))
        payload = b"[" + b",".join(items) + b"]"
        compressed_payload = gzip_compress(payload)
        headers = {
            "Date": email.utils.formatdate(usegmt=True),
            "User-Agent": "baseplate-event-publisher/1.0",
            "Content-Type": "application/json",
            "X-Signature": self._sign_payload(payload),
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
                if getattr(exc, "response", None) and exc.response.status_code < 500:
                    raise
            except IOError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                self.metrics.counter("sent").increment(len(items))
                return

            time.sleep(RETRY_DELAY)


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

    config_parser = configparser.ConfigParser()
    config_parser.readfp(args.config_file)
    raw_config = dict(config_parser.items("event-publisher:" + args.queue_name))
    cfg = config.parse_config(raw_config, {
        "collector": {
            "hostname": config.String,
        },

        "key": {
            "name": config.String,
            "secret": config.Base64,
        },
    })

    metrics_client = make_metrics_client(raw_config)

    consumer = BatchConsumer(metrics_client, cfg)
    batcher = Batcher(consumer)
    event_queue = MessageQueue(
        "/events-" + args.queue_name,
        max_messages=MAX_QUEUE_SIZE,
        max_message_size=MAX_EVENT_SIZE,
    )

    try:
        while True:
            try:
                message = event_queue.get(timeout=.2)
            except TimedOutError:
                pass
            else:
                batcher.add(message)

            if batcher.batch_age > MAX_BATCH_AGE:
                batcher.flush()
    finally:
        batcher.flush()


if __name__ == "__main__":
    publish_events()
