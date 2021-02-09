import argparse
import configparser
import logging
import urllib.parse

from typing import Optional

import requests

from baseplate import __version__ as baseplate_version
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.lib.metrics import metrics_client_from_config
from baseplate.lib.retry import RetryPolicy
from baseplate.observers.tracing import MAX_QUEUE_SIZE
from baseplate.observers.tracing import MAX_SPAN_SIZE
from baseplate.sidecars import BatchFull
from baseplate.sidecars import RawJSONBatch
from baseplate.sidecars import SerializedBatch
from baseplate.sidecars import TimeLimitedBatch


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
    def __init__(self, max_size: int = MAX_BATCH_SIZE_DEFAULT):
        super().__init__(max_size)


class ZipkinPublisher:
    """Zipkin trace publisher."""

    def __init__(
        self,
        zipkin_api_url: str,
        metrics_client: metrics.Client,
        post_timeout: int = POST_TIMEOUT_DEFAULT,
        retry_limit: int = RETRY_LIMIT_DEFAULT,
        num_conns: int = 5,
    ):

        adapter = requests.adapters.HTTPAdapter(pool_connections=num_conns, pool_maxsize=num_conns)
        parsed_url = urllib.parse.urlparse(zipkin_api_url)
        self.session = requests.Session()
        self.session.headers[
            "User-Agent"
        ] = f"baseplate.py-{self.__class__.__name__}/{baseplate_version}"
        self.session.mount(f"{parsed_url.scheme}://", adapter)
        self.endpoint = f"{zipkin_api_url}/spans"
        self.metrics = metrics_client
        self.post_timeout = post_timeout
        self.retry_limit = retry_limit

    def publish(self, payload: SerializedBatch) -> None:
        """Publish spans to Zipkin API.

        :param payload: Count and payload to publish.
        """
        if not payload.item_count:
            return

        logger.info("Sending batch of %d traces", payload.item_count)
        headers = {
            "User-Agent": "baseplate-trace-publisher/1.0",
            "Content-Type": "application/json",
        }
        for _ in RetryPolicy.new(attempts=self.retry_limit):
            try:
                with self.metrics.timer("post"):
                    response = self.session.post(
                        self.endpoint,
                        data=payload.serialized,
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
            except OSError:
                self.metrics.counter("error.io").increment()
                logger.exception("HTTP Request failed")
            else:
                self.metrics.counter("sent").increment(payload.item_count)
                return

        raise MaxRetriesError(
            f"ZipkinPublisher exhausted allowance of {self.retry_limit:d} retries."
        )


def publish_traces() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    arg_parser.add_argument(
        "--queue-name",
        default="main",
        help="name of trace queue / publisher config (default: main)",
    )
    arg_parser.add_argument(
        "--debug", default=False, action="store_true", help="enable debug logging"
    )
    arg_parser.add_argument(
        "--app-name",
        default="main",
        metavar="NAME",
        help="name of app to load from config_file (default: main)",
    )
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level)

    config_parser = configparser.RawConfigParser()
    config_parser.read_file(args.config_file)

    publisher_raw_cfg = dict(config_parser.items("trace-publisher:" + args.queue_name))
    publisher_cfg = config.parse_config(
        publisher_raw_cfg,
        {
            "zipkin_api_url": config.DefaultFromEnv(config.Endpoint, "BASEPLATE_ZIPKIN_API_URL"),
            "post_timeout": config.Optional(config.Integer, POST_TIMEOUT_DEFAULT),
            "max_batch_size": config.Optional(config.Integer, MAX_BATCH_SIZE_DEFAULT),
            "retry_limit": config.Optional(config.Integer, RETRY_LIMIT_DEFAULT),
            "max_queue_size": config.Optional(config.Integer, MAX_QUEUE_SIZE),
        },
    )

    trace_queue = MessageQueue(
        "/traces-" + args.queue_name,
        max_messages=publisher_cfg.max_queue_size,
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
        message: Optional[bytes]

        try:
            message = trace_queue.get(timeout=0.2)
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
