import argparse
import logging

import requests

from . import MAX_SPAN_SIZE, MAX_QUEUE_SIZE
from baseplate import config, metrics_client_from_config
from baseplate._compat import configparser
from baseplate.message_queue import MessageQueue, TimedOutError
from baseplate.retry import RetryPolicy
from baseplate._compat import urlparse
from baseplate._utils import BatchFull, RawJSONBatch, TimeLimitedBatch


logger = logging.getLogger(__name__)

# seconds to wait for response from Zipkin server
POST_TIMEOUT_DEFAULT = 3

# maximum size (in bytes) of a batch of traces
MAX_BATCH_SIZE_DEFAULT = 500 * 1024
# maximum time (seconds) a traces can sit around while we wait for more
# messages to batch
MAX_BATCH_AGE = 1

# maximum number of retries when publishing traces
RETRY_LIMIT_DEFAULT = 10


class MaxRetriesError(Exception):
    pass


class TraceBatch(RawJSONBatch):
    def __init__(self, max_size=MAX_BATCH_SIZE_DEFAULT):
        super(TraceBatch, self).__init__(max_size)


class ZipkinPublisher(object):
    """Zipkin trace publisher."""
    def __init__(self,
            zipkin_api_url,
            metrics_client,
            post_timeout=POST_TIMEOUT_DEFAULT,
            retry_limit=RETRY_LIMIT_DEFAULT,
            num_conns=5):

        adapter = requests.adapters.HTTPAdapter(pool_connections=num_conns,
                                                pool_maxsize=num_conns)
        parsed_url = urlparse(zipkin_api_url)
        self.session = requests.Session()
        self.session.mount("{}://".format(parsed_url.scheme), adapter)
        self.endpoint = "{}/spans".format(zipkin_api_url)
        self.metrics = metrics_client
        self.post_timeout = post_timeout
        self.retry_limit = retry_limit

    def publish(self, payload):
        """Publish spans to Zipkin API.

        :param baseplate.events.publisher.SerializedBatch payload: Count and
            payload to publish.
        """
        if not payload.count:
            return

        logger.info("Sending batch of %d traces", payload.count)
        headers = {
            "User-Agent": "baseplate-trace-publisher/1.0",
            "Content-Type": "application/json",
        }
        for time_remaining in RetryPolicy.new(attempts=self.retry_limit):
            try:
                with self.metrics.timer("post"):
                    response = self.session.post(
                        self.endpoint,
                        data=payload.bytes,
                        headers=headers,
                        timeout=self.post_timeout,
                        stream=False,
                    )
                response.raise_for_status()
            except requests.HTTPError as exc:
                self.metrics.counter("error.http").increment()
                response = getattr(exc, "response", None)
                if response is not None:
                    logger.exception("HTTP Request failed. Error: %s", response.text)
                    # If client error, crash
                    if response.status_code < 500:
                        raise
                else:
                    logger.exception("HTTP Request failed. Response not available")
            except IOError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                self.metrics.counter("sent").increment(payload.count)
                return
        else:
            raise MaxRetriesError(
                "ZipkinPublisher exhausted allowance of %d retries.",
                self.retry_limit)


def publish_traces():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    arg_parser.add_argument("--queue-name", default="main",
        help="name of trace queue / publisher config (default: main)")
    arg_parser.add_argument("--debug", default=False, action="store_true",
        help="enable debug logging")
    arg_parser.add_argument("--app-name", default="main", metavar="NAME",
        help="name of app to load from config_file (default: main)")
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level)

    config_parser = configparser.RawConfigParser()
    config_parser.readfp(args.config_file)

    publisher_raw_cfg = dict(config_parser.items("trace-publisher:" + args.queue_name))
    publisher_cfg = config.parse_config(publisher_raw_cfg, {
        "zipkin_api_url": config.Endpoint,
        "post_timeout": config.Optional(config.Integer, POST_TIMEOUT_DEFAULT),
        "max_batch_size": config.Optional(config.Integer, MAX_BATCH_SIZE_DEFAULT),
        "retry_limit": config.Optional(config.Integer, RETRY_LIMIT_DEFAULT),
    })

    trace_queue = MessageQueue(
        "/traces-" + args.queue_name,
        max_messages=MAX_QUEUE_SIZE,
        max_message_size=MAX_SPAN_SIZE,
    )

    # pylint: disable=maybe-no-member
    inner_batch = TraceBatch(max_size=publisher_cfg.max_batch_size)
    batcher = TimeLimitedBatch(inner_batch, MAX_BATCH_AGE)
    metrics_client = metrics_client_from_config(publisher_raw_cfg)
    publisher = ZipkinPublisher(
        publisher_cfg.zipkin_api_url.address,
        metrics_client,
        post_timeout=publisher_cfg.post_timeout,
    )

    while True:
        try:
            message = trace_queue.get(timeout=.2)
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
    publish_traces()
