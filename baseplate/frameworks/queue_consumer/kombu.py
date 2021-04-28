import logging
import queue
import socket

from datetime import timedelta
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

import kombu

from gevent.server import StreamServer
from kombu.mixins import ConsumerMixin
from kombu.transport.virtual import Channel

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate.clients.kombu import KombuSerializer
from baseplate.lib.batched_queue import BatchedQueue
from baseplate.server.queue_consumer import HealthcheckCallback
from baseplate.server.queue_consumer import make_simple_healthchecker
from baseplate.server.queue_consumer import MessageHandler
from baseplate.server.queue_consumer import PumpWorker
from baseplate.server.queue_consumer import QueueConsumerFactory


if TYPE_CHECKING:
    WorkQueue = queue.Queue[kombu.Message]  # pylint: disable=unsubscriptable-object
    BatchWorkQueue = BatchedQueue[kombu.Message]
else:
    WorkQueue = queue.Queue
    BatchWorkQueue = BatchedQueue

logger = logging.getLogger(__name__)


Handler = Callable[[RequestContext, Any, kombu.Message], None]
ErrorHandler = Callable[[RequestContext, Any, kombu.Message, Exception], None]

BatchHandler = Callable[[RequestContext, Sequence[kombu.Message]], None]
BatchErrorHandler = Callable[[RequestContext, Sequence[kombu.Message], Exception], None]


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

    def get_consumers(self, consumer: kombu.Consumer, channel: Channel) -> Sequence[kombu.Consumer]:
        args = dict(queues=self.queues, on_message=self.work_queue.put, **self.kwargs)
        if self.serializer:
            args["accept"] = [self.serializer.name]
        return [consumer(**args)]

    def stop(self) -> None:
        logger.debug("Closing KombuConsumerWorker.")
        # `should_stop` is an attribute of `ConsumerMixin`
        self.should_stop = True


class KombuBatchConsumerWorker(ConsumerMixin, PumpWorker):
    def __init__(
        self,
        connection: kombu.Connection,
        queues: Sequence[kombu.Queue],
        work_queue: BatchedQueue,
        serializer: Optional[KombuSerializer] = None,
    ) -> None:
        self.connection = connection
        self.queues = queues
        self.work_queue = work_queue
        self.serializer = serializer

    def get_consumers(self, consumer: kombu.Consumer, channel: Channel) -> Sequence[kombu.Consumer]:
        args = dict(queues=self.queues, on_message=self.work_queue.put)
        if self.serializer:
            args["accept"] = [self.serializer.name]
        return [consumer(**args)]

    def stop(self) -> None:
        logger.debug("Closing KombuBatchConsumerWorker.")
        # `should_stop` is an attribute of `ConsumerMixin`
        self.should_stop = True

        messages = self.work_queue.drain()

        if messages:
            logger.debug("Requeueing %i messages", len(messages))
            message: kombu.Message
            for message in messages:
                if not message.acknowledged:
                    message.requeue()
                else:
                    logger.warning(
                        "A message in the unprocessed batch has already been acknowledged."
                    )


class KombuMessageHandler(MessageHandler):
    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        handler_fn: Handler,
        error_handler_fn: Optional[ErrorHandler] = None,
    ):
        self.baseplate = baseplate
        self.name = name
        self.handler_fn = handler_fn
        self.error_handler_fn = error_handler_fn

    def handle(self, message: kombu.Message) -> None:
        context = self.baseplate.make_context_object()
        try:
            # We place the call to ``baseplate.make_server_span`` inside the
            # try/except block because we still want Baseplate to see and
            # handle the error (publish it to error reporting)
            with self.baseplate.make_server_span(context, self.name) as span:
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
            logger.exception(
                "Unhandled error while trying to process a message. The message "
                "has been returned to the queue broker."
            )
            if self.error_handler_fn:
                self.error_handler_fn(context, message_body, message, exc)
            else:
                message.requeue()

            if isinstance(exc, FatalMessageHandlerError):
                logger.info("Recieved a fatal error, terminating the server.")
                raise
        else:
            message.ack()


class KombuBatchMessageHandler(MessageHandler):
    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        handler_fn: BatchHandler,
        error_handler_fn: Optional[BatchErrorHandler] = None,
    ):
        self.baseplate = baseplate
        self.name = name
        self.handler_fn = handler_fn
        self.error_handler_fn = error_handler_fn

    def handle(self, messages: Sequence[kombu.Message]) -> None:  # pylint: disable=arguments-differ
        logger.debug("Processing batch with %i messages", len(messages))

        context = self.baseplate.make_context_object()
        try:
            # We place the call to ``baseplate.make_server_span`` inside the
            # try/except block because we still want Baseplate to see and
            # handle the error (publish it to error reporting)
            with self.baseplate.make_server_span(context, self.name) as span:
                span.set_tag("kind", "batch_consumer")
                span.set_tag("size", len(messages))

                # The routing keys, consumer tags, delivery tags and exchanges span tags are
                # comma-separated, de-duplicated lists of the tags in the messages

                routing_keys = set(None)
                consumer_tags = set(None)
                delivery_tags = set(None)
                exchanges = set(None)

                for message in messages:
                    message: kombu.Message
                    delivery_info = message.delivery_info
                    routing_key = delivery_info.get("routing_key", None)
                    routing_keys.add(routing_key)
                    consumer_tag = delivery_info.get("consumer_tag", None)
                    consumer_tags.add(consumer_tag)
                    delivery_tag = delivery_info.get("delivery_tag", None)
                    delivery_tags.add(delivery_tag)
                    exchange = delivery_info.get("exchange", None)
                    exchanges.add(exchange)
                
                routing_keys.remove(None)
                consumer_tags.remove(None)
                delivery_tags.remove(None)
                exchanges.remove(None)

                routing_keys_span_tag = ",".join(item in routing_keys)
                span.set_tag("amqp.routing_keys", routing_keys_span_tag)
                consumer_tags_span_tag = ",".join(item in consumer_tags)
                span.set_tag("amqp.consumer_tags", consumer_tags_span_tag)
                delivery_tags_span_tag = ",".join(item in delivery_tags)
                span.set_tag("amqp.delivery_tags", delivery_tags_span_tag)
                exchanges_span_tag = ",".join(item in exchanges)
                span.set_tag("amqp.exchanges", exchanges_span_tag)

                self.handler_fn(context, list(messages))
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(
                "Unhandled error while trying to process a message.  The message "
                "has been returned to the queue broker."
            )
            if self.error_handler_fn:
                self.error_handler_fn(context, messages, exc)
            else:
                for message in messages:
                    if not message.acknowledged:
                        message.requeue()

            if isinstance(exc, FatalMessageHandlerError):
                logger.info("Received a fatal error, terminating the server.")
                raise
        else:
            for message in messages:
                if not message.acknowledged:
                    message.ack()
            logger.debug("Successfully processed batch with %i messages", len(messages))


class KombuQueueConsumerFactory(QueueConsumerFactory):
    """Factory for running a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer` using Kombu.

    For simple cases where you just need a basic queue with all the default
    parameters for your message broker, you can use `KombuQueueConsumerFactory.new`.

    If you need more control, you can create the :py:class:`~kombu.Queue` objects yourself and
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
        worker_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """`KombuQueueConsumerFactory` constructor.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your :py:class:`~kombu.Queue` objects
            to.
        :param queues: List of  :py:class:`~kombu.Queue` objects to consume from.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create :py:class:`~kombu.Queue` objects
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
        worker_kwargs: Optional[Dict[str, Any]] = None,
    ) -> "KombuQueueConsumerFactory":
        """Return a new `KombuQueueConsumerFactory`.

        This method will create the :py:class:`~kombu.Queue` objects for you and is
        appropriate to use in simple cases where you just need a basic queue with
        all the default parameters for your message broker.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your
            :py:class:`~kombu.Queue` objects to.
        :param exchange: The `kombu.Connection` to your message broker.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create
            :py:class:`~kombu.Queue` objects to consume from.
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
            self.baseplate, self.name, self.handler_fn, self.error_handler_fn
        )

    def build_health_checker(self, listener: socket.socket) -> StreamServer:
        return make_simple_healthchecker(listener, callback=self.health_check_fn)


class KombuBatchQueueConsumerFactory(QueueConsumerFactory):
    """Factory for running a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer` using Kombu with batch processing.

    For simple cases where you just need a basic queue with all the default
    parameters for your message broker, you can use `KombuQueueConsumerFactory.new`.

    If you need more control, you can create the :py:class:`~kombu.Queue` objects yourself and
    use the constructor directly.
    """

    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        connection: kombu.Connection,
        queues: Sequence[kombu.Queue],
        handler_fn: BatchHandler,
        error_handler_fn: Optional[BatchErrorHandler] = None,
        health_check_fn: Optional[HealthcheckCallback] = None,
        serializer: Optional[KombuSerializer] = None,
        batch_size: int = 1,
        batch_timeout: timedelta = timedelta(seconds=1),
    ) -> None:
        """`KombuQueueConsumerFactory` constructor.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your :py:class:`~kombu.Queue` objects
            to.
        :param queues: List of  :py:class:`~kombu.Queue` objects to consume from.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create :py:class:`~kombu.Queue` objects
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
        :param batch_size: The size of a message batch
        :param batch_timeout: The timeout after which a message will latest be processed even if
            the batch is not full
        """
        self.baseplate = baseplate
        self.connection = connection
        self.queues = queues
        self.name = name
        self.handler_fn = handler_fn
        self.error_handler_fn = error_handler_fn
        self.health_check_fn = health_check_fn
        self.serializer = serializer
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    @classmethod
    def new(
        cls,
        baseplate: Baseplate,
        exchange: kombu.Exchange,
        connection: kombu.Connection,
        queue_name: str,
        routing_keys: Sequence[str],
        handler_fn: BatchHandler,
        error_handler_fn: Optional[BatchErrorHandler] = None,
        health_check_fn: Optional[HealthcheckCallback] = None,
        serializer: Optional[KombuSerializer] = None,
        batch_size: int = 1,
        batch_timeout: timedelta = timedelta(seconds=1),
    ) -> "KombuBatchQueueConsumerFactory":
        """Return a new `KombuBatchQueueConsumerFactory`.

        This method will create the :py:class:`~kombu.Queue` objects for you and is
        appropriate to use in simple cases where you just need a basic queue with
        all the default parameters for your message broker.

        :param baseplate: The Baseplate set up for your consumer.
        :param exchange: The `kombu.Exchange` that you will bind your
            :py:class:`~kombu.Queue` objects to.
        :param exchange: The `kombu.Connection` to your message broker.
        :param queue_name: Name for your queue.
        :param routing_keys: List of routing keys that you will create
            :py:class:`~kombu.Queue` objects to consume from.
        :param handler_fn: A function that will process an individual message from a queue.
        :param error_handler_fn: A function that will be called when an error is thrown
            while executing the `handler_fn`. This function will be responsible for calling
            `message.ack` or `message.requeue` as it will not be automatically called by
            `KombuMessageHandler`'s `handle` function.
        :param health_check_fn: A `baseplate.server.queue_consumer.HealthcheckCallback`
            function that can be used to customize your health check.
        :param serializer: A `baseplate.clients.kombu.KombuSerializer` that should
            be used to decode the messages you are consuming.
        :param batch_size: The size of a message batch
        :param batch_timeout: The timeout after which a message will latest be processed even if
            the batch is not full
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
            batch_size=batch_size,
            batch_timeout=batch_timeout,
        )

    def build_pump_worker(self, work_queue: queue.Queue) -> KombuBatchConsumerWorker:
        batched_queue: BatchedQueue[kombu.Message] = BatchedQueue(
            work_queue, self.batch_size, self.batch_timeout
        )
        return KombuBatchConsumerWorker(
            connection=self.connection,
            queues=self.queues,
            work_queue=batched_queue,
            serializer=self.serializer,
        )

    def build_message_handler(self) -> KombuBatchMessageHandler:
        return KombuBatchMessageHandler(
            self.baseplate, self.name, self.handler_fn, self.error_handler_fn,
        )

    def build_health_checker(self, listener: socket.socket) -> StreamServer:
        return make_simple_healthchecker(listener, callback=self.health_check_fn)
