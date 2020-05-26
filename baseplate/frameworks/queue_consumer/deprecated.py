import logging
import queue

from threading import Thread
from typing import Callable
from typing import NoReturn
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

from kombu import Connection
from kombu import Exchange
from kombu import Message
from kombu import Queue

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import Span
from baseplate.frameworks.queue_consumer.kombu import KombuConsumerWorker
from baseplate.lib import warn_deprecated
from baseplate.lib.retry import RetryPolicy

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    WorkQueue = queue.Queue[Message]  # pylint: disable=unsubscriptable-object
else:
    WorkQueue = queue.Queue


Handler = Callable[[RequestContext, str, Message], None]


def consume(
    baseplate: Baseplate,
    exchange: Exchange,
    connection: Connection,
    queue_name: str,
    routing_keys: Sequence[str],
    handler: Handler,
) -> NoReturn:
    """Create a long-running process to consume messages from a queue.

    A queue with name ``queue_name`` is created and bound to the
    ``routing_keys`` so messages published to the ``routing_keys`` are routed
    to the queue.

    Next, the process registers a consumer that receives messages from
    the queue and feeds them to the ``handler``.

    The ``handler`` function must take 3 arguments:

    * ``context``: a baseplate context
    * ``message_body``: the text body of the message
    * ``message``: :py:class:`kombu.message.Message`

    The consumer will automatically ``ack`` each message after the handler
    method exits. If there is an error in processing and the message must be
    retried the handler should raise an exception to crash the process. This
    will prevent the ``ack`` and the message will be re-queued at the head of
    the queue.

    :param baseplate: A baseplate instance for the service.
    :param exchange:
    :param connection:
    :param queue_name: The name of the queue.
    :param routing_keys: List of routing keys.
    :param handler: The handler method.

    """
    warn_deprecated(
        "baseplate.frameworks.queue_consumer is deprecated and will be removed "
        "in the next major release.  You should migrate your consumers to use "
        "baseplate.server.queue_consumer.\n"
        "https://baseplate.readthedocs.io/en/stable/api/baseplate/frameworks/queue_consumer/deprecated.html"
    )
    queues = []
    for routing_key in routing_keys:
        queues.append(Queue(name=queue_name, exchange=exchange, routing_key=routing_key))

    logger.info("registering %s as a handler for %r", handler.__name__, queues)
    kombu_consumer = KombuConsumer.new(connection, queues)

    logger.info("waiting for messages")
    while True:
        context = baseplate.make_context_object()
        with baseplate.make_server_span(context, queue_name) as span:
            message = kombu_consumer.get_message(span)
            handler(context, message.body, message)
            message.ack()


class BaseKombuConsumer:
    """Base object for consuming messages from a queue.

    A worker process accepts messages from the queue and puts them in a local
    work queue. The "real" consumer can then get messages with
    :py:meth:`~baseplate.frameworks.queue_consumer.BaseKombuConsumer.get_message` or
    :py:meth:`~baseplate.frameworks.queue_consumer.BaseKombuConsumer.get_batch`. It is
    that consumer's responsibility to ``ack`` or ``reject`` messages.

    Can be used directly, outside of standard baseplate context.

    """

    def __init__(self, worker: KombuConsumerWorker, worker_thread: Thread, work_queue: WorkQueue):
        self.worker = worker
        self.worker_thread = worker_thread
        self.work_queue = work_queue

    @classmethod
    def new(
        cls, connection: Connection, queues: Sequence[Queue], queue_size: int = 100
    ) -> "BaseKombuConsumer":
        """Create and initialize a consumer.

        :param connection: The connection
        :param queues: List of queues.
        :param queue_size: The maximum number of messages to cache
            in the internal `queue.Queue` worker queue.  Defaults to 100.  For
            an infinite size (not recommended), use `queue_size=0`.

        """
        work_queue: WorkQueue = queue.Queue(maxsize=queue_size)
        worker = KombuConsumerWorker(connection, queues, work_queue)
        worker_thread = Thread(target=worker.run)
        worker_thread.name = "consumer message pump"
        worker_thread.daemon = True
        worker_thread.start()

        return cls(worker, worker_thread, work_queue)

    def get_message(self, timeout: Optional[float] = None) -> Message:
        """Return a single message."""
        batch = self.get_batch(max_items=1, timeout=timeout)
        return batch[0]

    def get_batch(self, max_items: int, timeout: Optional[float]) -> Sequence[Message]:
        """Return a batch of messages.

        :param max_items: The maximum batch size.
        :param timeout: The maximum time to wait in seconds, or ``None``
            for no timeout.

        """
        if timeout == 0:
            block = False
        else:
            block = True
        batch = []
        retry_policy = RetryPolicy.new(attempts=max_items, budget=timeout)
        for time_remaining in retry_policy:
            item = self._get_next_item(block=block, timeout=time_remaining)
            if item is None:
                break
            batch.append(item)

        return batch

    def _get_next_item(self, block: bool, timeout: Optional[float]) -> Optional[Message]:
        try:
            return self.work_queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None


class KombuConsumer:
    """Consumer for use in baseplate.

    The :py:meth:`~baseplate.frameworks.queue_consumer.KombuConsumer.get_message` and
    :py:meth:`~baseplate.frameworks.queue_consumer.KombuConsumer.get_batch` methods will
    automatically record diagnostic information.

    """

    def __init__(self, base_consumer: BaseKombuConsumer):
        self.base_consumer = base_consumer

    @classmethod
    def new(
        cls, connection: Connection, queues: Sequence[Queue], queue_size: int = 100
    ) -> "KombuConsumer":
        """Create and initialize a consumer.

        :param connection: The connection
        :param queues: List of queues.
        :param queue_size: The maximum number of messages to cache
            in the internal `queue.Queue` worker queue.  Defaults to 100.  For
            an infinite size (not recommended), use `queue_size=0`.

        """
        base_consumer = BaseKombuConsumer.new(connection, queues, queue_size)
        return cls(base_consumer)

    def get_message(self, server_span: Span) -> Message:
        """Return a single message.

        :param server_span: The span.

        """
        child_span = server_span.make_child("kombu.get_message")
        child_span.set_tag("kind", "consumer")

        with child_span:
            messages = self.base_consumer.get_batch(max_items=1, timeout=None)
            message = messages[0]

            routing_key = message.delivery_info.get("routing_key", "")
            child_span.set_tag("routing_key", routing_key)

            consumer_tag = message.delivery_info.get("consumer_tag", "")
            child_span.set_tag("consumer_tag", consumer_tag)

            delivery_tag = message.delivery_info.get("delivery_tag", "")
            child_span.set_tag("delivery_tag", delivery_tag)

            exchange = message.delivery_info.get("exchange", "")
            child_span.set_tag("exchange", exchange)

            return message

    def get_batch(
        self, server_span: Span, max_items: int, timeout: Optional[float]
    ) -> Sequence[Message]:
        """Return a batch of messages.

        :param server_span: The span.
        :param max_items: The maximum batch size.
        :param timeout: The maximum time to wait in seconds, or ``None``
            for no timeout.

        """
        child_span = server_span.make_child("kombu.get_batch")
        child_span.set_tag("kind", "consumer")

        with child_span:
            messages = self.base_consumer.get_batch(max_items, timeout)
            child_span.set_tag("message_count", len(messages))
            return messages
