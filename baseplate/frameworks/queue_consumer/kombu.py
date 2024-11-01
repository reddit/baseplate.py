import logging
import queue
import socket
import time
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, NamedTuple, Optional

import kombu
from gevent.server import StreamServer
from kombu.mixins import ConsumerMixin
from kombu.transport.virtual import Channel
from prometheus_client import Counter, Gauge, Histogram

from baseplate import Baseplate, RequestContext
from baseplate.clients.kombu import KombuSerializer
from baseplate.lib.errors import KnownException
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.server.queue_consumer import (
    HealthcheckCallback,
    MessageHandler,
    PumpWorker,
    QueueConsumerFactory,
    make_simple_healthchecker,
)


class AmqpConsumerPrometheusLabels(NamedTuple):
    amqp_address: str
    amqp_virtual_host: str
    amqp_exchange_name: str
    amqp_routing_key: str


AMQP_PROCESSING_TIME = Histogram(
    "amqp_consumer_message_processing_time_seconds",
    "latency histogram of how long it takes to process a message",
    AmqpConsumerPrometheusLabels._fields + ("amqp_success",),
    buckets=default_latency_buckets,
)

AMQP_PROCESSED_TOTAL = Counter(
    "amqp_consumer_messages_processed_total",
    "total count of messages processed by this host",
    AmqpConsumerPrometheusLabels._fields + ("amqp_success",),
)

AMQP_REPUBLISHED_TOTAL = Counter(
    "amqp_consumer_messages_republished_total",
    "total count of messages republished by this host",
    AmqpConsumerPrometheusLabels._fields,
)

AMQP_REJECTED_REASON_TTL = "ttl"
AMQP_REJECTED_REASON_RETRIES = "retries"
AMQP_REJECTED_TOTAL = Counter(
    "amqp_consumer_messages_rejected_total",
    "total count of messages that were rejected by this host",
    AmqpConsumerPrometheusLabels._fields + ("reason_code",),
)

AMQP_TTL_REACHED_TOTAL = Counter(
    "amqp_consumer_message_ttl_reached_total",
    "total count of messages that reached the ttl and were discarded by this host",
    AmqpConsumerPrometheusLabels._fields,
)

AMQP_ACTIVE_MESSAGES = Gauge(
    "amqp_consumer_active_messages",
    "gauge that reflects the number of messages currently being processed",
    AmqpConsumerPrometheusLabels._fields,
    multiprocess_mode="livesum",
)

if TYPE_CHECKING:
    WorkQueue = queue.Queue[kombu.Message]  # pylint: disable=unsubscriptable-object
else:
    WorkQueue = queue.Queue

logger = logging.getLogger(__name__)


Handler = Callable[[RequestContext, Any, kombu.Message], None]
ErrorHandler = Callable[[RequestContext, Any, kombu.Message, Exception], None]


class FatalMessageHandlerError(Exception):
    """An error that signals that the queue process should exit.

    Raising an Exception that is a subclass of FatalMessageHandlerError will
    cause the KombuMessageHandler to re-raise the exception rather than swallowing
    it which will cause the handler thread/process to stop.  This, in turn, will
    gracefully shut down the QueueConsumerServer currently running.

    Exceptions of this nature should be reserved for errors that are due to
    problems with the environment rather than the message itself.  For example,
    a node that cannot get its AWS credentials.
    """


MESSAGE_HEADER_RETRY_COUNT = "x-retry-count"
MESSAGE_HEADER_RETRY_LIMIT = "x-retry-limit"
MESSAGE_HEADER_TTL = "x-ttl"


class RetryMode(Enum):
    """REQUEUE - backward compatible behavior; the message is returned into the queue.
    RabbitMQ puts it into the head of a queue and attempts to send it to consumer ASAP.

    REPUBLISH - message is acknowledged. New message is created, having identical content,
    but incremented retry counter. It is published into the tail of a queue.
    """

    REQUEUE = (1,)
    REPUBLISH = (2,)


class KombuConsumerWorker(ConsumerMixin, PumpWorker):
    """Consumes messages from the given queues and pumps them into the internal work_queue.

    This class does not directly implement the abstract `run` command from
    PumpWorker because the ConsumerMixin class already defines it.
    """

    def __init__(
        self,
        connection: kombu.Connection,
        queues: Sequence[kombu.Queue],
        work_queue: WorkQueue,
        serializer: Optional[KombuSerializer] = None,
        **kwargs: Any,
    ):
        self.connection = connection
        self.queues = queues
        self.work_queue = work_queue
        self.serializer = serializer
        self.kwargs = kwargs

    def get_consumers(self, Consumer: kombu.Consumer, channel: Channel) -> Sequence[kombu.Consumer]:
        args = {
            "queues": self.queues,
            "on_message": self.work_queue.put,
            **self.kwargs,
        }
        if self.serializer:
            args["accept"] = [self.serializer.name]
        return [Consumer(**args)]

    def stop(self) -> None:
        logger.debug("Closing KombuConsumerWorker.")
        # `should_stop` is an attribute of `ConsumerMixin`
        self.should_stop = True


class KombuMessageHandler(MessageHandler):
    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        handler_fn: Handler,
        error_handler_fn: Optional[ErrorHandler] = None,
        retry_mode: RetryMode = RetryMode.REQUEUE,
        retry_limit: Optional[int] = None,
    ):
        self.baseplate = baseplate
        self.name = name
        self.handler_fn = handler_fn
        self.error_handler_fn = error_handler_fn
        self.retry_mode = retry_mode
        self.retry_limit = retry_limit

    def _is_error_recoverable(self, exc: Exception) -> bool:
        if isinstance(exc, KnownException):
            return exc.is_recoverable()
        return True  # for backward compatibility, retry unexpected errors

    def _handle_error(
        self,
        message: kombu.Message,
        prometheus_labels: AmqpConsumerPrometheusLabels,
        exc: Exception,
    ) -> None:
        if not self._is_error_recoverable(exc):
            message.reject()
            logger.exception(
                "Unrecoverable error while trying to process a message.  The message has been discarded."  # noqa: E501
            )
            return

        message_exchange = message.delivery_info.get("exchange")
        message_routing_key = message.delivery_info.get("routing_key")
        if not message_exchange or not message_routing_key or self.retry_mode == RetryMode.REQUEUE:
            logger.exception(
                "Recoverable error while trying to process a message. "
                "The message has been returned to the queue broker."
            )
            message.requeue()
            return

        headers = message.headers or {}
        retry_count_val = headers.get(MESSAGE_HEADER_RETRY_COUNT, 0)
        try:
            retry_count = int(retry_count_val)
        except (ValueError, TypeError):
            retry_count = 0

        retry_limit_val = headers.get(MESSAGE_HEADER_RETRY_LIMIT, None)
        retry_limit: Optional[int]
        try:
            retry_limit = int(retry_limit_val)
        except (ValueError, TypeError):
            retry_limit = None

        if (self.retry_limit is not None and retry_count >= self.retry_limit) or (
            retry_limit is not None and retry_count >= retry_limit
        ):
            logger.exception(
                "Unhandled error while trying to process a message. "
                "The message reached the retry limit."
            )
            AMQP_REJECTED_TOTAL.labels(
                **prometheus_labels._asdict(), reason_code=AMQP_REJECTED_REASON_RETRIES
            ).inc()
            message.reject()
            return

        headers[MESSAGE_HEADER_RETRY_COUNT] = retry_count + 1
        message.ack()

        new_message = message.channel.prepare_message(
            message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            headers=headers,
        )

        message.channel.basic_publish(new_message, message_exchange, message_routing_key)
        AMQP_REPUBLISHED_TOTAL.labels(**prometheus_labels._asdict()).inc()
        logger.exception(
            "Unhandled error while trying to process a message. "
            "The retry message has been published to the queue broker."
        )

    def _is_ttl_over(self, message: kombu.Message) -> bool:
        ttl = (message.headers or {}).get(MESSAGE_HEADER_TTL, 0)
        return ttl and ttl < time.time()

    def handle(self, message: kombu.Message) -> None:
        start_time = time.perf_counter()
        prometheus_success = "true"
        prometheus_labels = AmqpConsumerPrometheusLabels(
            # note: localhost will be translated to 127.0.0.1 by the library
            amqp_address=message.channel.connection.client.host,
            amqp_virtual_host=message.channel.connection.client.virtual_host,
            amqp_exchange_name=message.delivery_info.get("exchange", ""),
            amqp_routing_key=message.delivery_info.get("routing_key", ""),
        )

        if self._is_ttl_over(message):
            message.reject()
            AMQP_REJECTED_TOTAL.labels(
                **prometheus_labels._asdict(), reason_code=AMQP_REJECTED_REASON_TTL
            ).inc()
            return

        context = self.baseplate.make_context_object()
        try:
            # We place the call to ``baseplate.make_server_span`` inside the
            # try/except block because we still want Baseplate to see and
            # handle the error (publish it to error reporting)
            with (
                self.baseplate.make_server_span(context, self.name) as span,
                AMQP_ACTIVE_MESSAGES.labels(**prometheus_labels._asdict()).track_inprogress(),
            ):
                delivery_info = message.delivery_info
                message_body = None
                message_body = message.decode()
                span.set_tag("kind", "consumer")
                span.set_tag("amqp.routing_key", delivery_info.get("routing_key", ""))
                span.set_tag("amqp.consumer_tag", delivery_info.get("consumer_tag", ""))
                span.set_tag("amqp.delivery_tag", delivery_info.get("delivery_tag", ""))
                span.set_tag("amqp.exchange", delivery_info.get("exchange", ""))
                self.handler_fn(context, message_body, message)
        except Exception as exc:
            prometheus_success = "false"

            # Custom error_handler_fn has priority over standard handler.
            if self.error_handler_fn:
                logger.debug(
                    "Unhandled error while trying to process a message. Custom handler invoked."
                )
                self.error_handler_fn(context, message_body, message, exc)
            else:
                self._handle_error(message, prometheus_labels, exc)

            if isinstance(exc, FatalMessageHandlerError):
                logger.info("Received a fatal error, terminating the server.")
                raise
        else:
            message.ack()
        finally:
            AMQP_PROCESSING_TIME.labels(
                **prometheus_labels._asdict(), amqp_success=prometheus_success
            ).observe(time.perf_counter() - start_time)
            AMQP_PROCESSED_TOTAL.labels(
                **prometheus_labels._asdict(), amqp_success=prometheus_success
            ).inc()


class KombuQueueConsumerFactory(QueueConsumerFactory):
    """Factory for running a
    :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer` using
    Kombu.

    For simple cases where you just need a basic queue with all the default
    parameters for your message broker, you can use `KombuQueueConsumerFactory.new`.

    If you need more control, you can create the :py:class:`~kombu.Queue` s yourself and
    use the constructor directly.
    """

    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        connection: kombu.Connection,
        queues: Sequence[kombu.Queue],
        handler_fn: Handler,
        error_handler_fn: Optional[ErrorHandler] = None,
        health_check_fn: Optional[HealthcheckCallback] = None,
        serializer: Optional[KombuSerializer] = None,
        worker_kwargs: Optional[dict[str, Any]] = None,
        retry_mode: RetryMode = RetryMode.REQUEUE,
        retry_limit: Optional[int] = None,
    ):
        """`KombuQueueConsumerFactory` constructor.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your :py:class:`~kombu.Queue` s
            to.
        :param queues: List of  :py:class:`~kombu.Queue` s to consume from.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create :py:class:`~kombu.Queue` s
            to consume from.
        :param handler_fn: A function that will process an individual message from a queue.
        :param error_handler_fn: A function that will be called when an error is thrown
            while executing the `handler_fn`. This function will be responsible for calling
            `message.ack` or `message.requeue` as it will not be automatically called by
            `KombuMessageHandler`'s `handle` function.
        :param health_check_fn: A `baseplate.server.queue_consumer.HealthcheckCallback`
            function that can be used to customize your health check.
        :param serializer: A `baseplate.clients.kombu.KombuSerializer` that should
            be used to decode the messages you are consuming.
        :param worker_kwargs: A dictionary of keyword arguments used to create queue consumers.
        :param retry_mode: Either RetryMode.REQUEUE (default): return message into the head of a
            queue, like old versions did. Or RetryMode.REPUBLISH: acknowledge the message and
            publish a new one, with the same content, but incremented retry counter.
        :param retry_limit: An number of retry attempts for the message. When the limit is reached,
            the message is discarded. Retry limit for specific message could also be specified in
            message's own header.
        """
        self.baseplate = baseplate
        self.connection = connection
        self.queues = queues
        self.name = name
        self.handler_fn = handler_fn
        self.error_handler_fn = error_handler_fn
        self.health_check_fn = health_check_fn
        self.serializer = serializer
        self.worker_kwargs = worker_kwargs
        self.retry_mode = retry_mode
        self.retry_limit = retry_limit

    @classmethod
    def new(
        cls,
        baseplate: Baseplate,
        exchange: kombu.Exchange,
        connection: kombu.Connection,
        queue_name: str,
        routing_keys: Sequence[str],
        handler_fn: Handler,
        error_handler_fn: Optional[ErrorHandler] = None,
        health_check_fn: Optional[HealthcheckCallback] = None,
        serializer: Optional[KombuSerializer] = None,
        worker_kwargs: Optional[dict[str, Any]] = None,
        retry_mode: RetryMode = RetryMode.REQUEUE,
        retry_limit: Optional[int] = None,
    ) -> "KombuQueueConsumerFactory":
        """Return a new `KombuQueueConsumerFactory`.

        This method will create the :py:class:`~kombu.Queue` s for you and is
        appropriate to use in simple cases where you just need a basic queue with
        all the default parameters for your message broker.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your
            :py:class:`~kombu.Queue` s to.
        :param exchange: The `kombu.Connection` to your message broker.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create
            :py:class:`~kombu.Queue` s to consume from.
        :param handler_fn: A function that will process an individual message from a queue.
        :param error_handler_fn: A function that will be called when an error is thrown
            while executing the `handler_fn`. This function will be responsible for calling
            `message.ack` or `message.requeue` as it will not be automatically called by
            `KombuMessageHandler`'s `handle` function.
        :param health_check_fn: A `baseplate.server.queue_consumer.HealthcheckCallback`
            function that can be used to customize your health check.
        :param serializer: A `baseplate.clients.kombu.KombuSerializer` that should
            be used to decode the messages you are consuming.
        :param worker_kwargs: A dictionary of keyword arguments used to configure a
            queue consumer.
        :param retry_mode: Either RetryMode.REQUEUE (default): return message into the head of a
            queue, like old versions did. Or RetryMode.REPUBLISH: acknowledge the message and
            publish a new one, with the same content, but incremented retry counter.
        :param retry_limit: An number of retry attempts for the message. When the limit is reached,
            the message is discarded.
        """
        queues = []
        for routing_key in routing_keys:
            queues.append(kombu.Queue(name=queue_name, exchange=exchange, routing_key=routing_key))
        return cls(
            baseplate=baseplate,
            name=queue_name,
            connection=connection,
            queues=queues,
            handler_fn=handler_fn,
            error_handler_fn=error_handler_fn,
            health_check_fn=health_check_fn,
            serializer=serializer,
            worker_kwargs=worker_kwargs,
            retry_mode=retry_mode,
            retry_limit=retry_limit,
        )

    def build_pump_worker(self, work_queue: WorkQueue) -> KombuConsumerWorker:
        kwargs = self.worker_kwargs or {}
        return KombuConsumerWorker(
            connection=self.connection,
            queues=self.queues,
            work_queue=work_queue,
            serializer=self.serializer,
            **kwargs,
        )

    def build_message_handler(self) -> KombuMessageHandler:
        return KombuMessageHandler(
            self.baseplate,
            self.name,
            self.handler_fn,
            self.error_handler_fn,
            self.retry_mode,
            self.retry_limit,
        )

    def build_health_checker(self, listener: socket.socket) -> StreamServer:
        return make_simple_healthchecker(listener, callback=self.health_check_fn)
