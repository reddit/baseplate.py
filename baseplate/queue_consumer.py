from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from threading import Thread

from baseplate._compat import queue
from baseplate.retry import RetryPolicy
from kombu import Queue
from kombu.mixins import ConsumerMixin


logger = logging.getLogger(__name__)


def consume(baseplate, exchange, connection, queue_name, routing_keys, handler):
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

    :param baseplate.core.Baseplate baseplate: A baseplate instance for the
        service.
    :param kombu.Exchange exchange:
    :param kombu.connection.Connection connection:
    :param str queue_name: The name of the queue.
    :param list routing_keys: List of routing keys.
    :param handler: The handler method.

    """
    queues = []
    for routing_key in routing_keys:
        queue = Queue(
            name=queue_name,
            exchange=exchange,
            routing_key=routing_key,
        )
        queues.append(queue)

    logger.info("registering %s as a handler for %r", handler.__name__, queues)
    kombu_consumer = KombuConsumer.new(connection, queues)

    logger.info("waiting for messages")
    while True:
        context = ConsumerContext()
        with baseplate.make_server_span(context, queue_name) as span:
            message = kombu_consumer.get_message(span)
            handler(context, message.body, message)
            message.ack()


class _ConsumerWorker(ConsumerMixin):
    def __init__(self, connection, queues, work_queue):
        self.connection = connection
        self.queues = queues
        self.work_queue = work_queue

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=self.queues,
            on_message=self.on_message,
        )]

    def on_message(self, message):
        self.work_queue.put(message)

    def get_message(self, block, timeout):
        try:
            return self.work_queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None


class ConsumerContext(object):
    pass


class BaseKombuConsumer(object):
    """Base object for consuming messages from a queue.

    A worker process accepts messages from the queue and puts them in a local
    work queue. The "real" consumer can then get messages with
    :py:meth:`~baseplate.queue_consumer.BaseKombuConsumer.get_message` or
    :py:meth:`~baseplate.queue_consumer.BaseKombuConsumer.get_batch`. It is
    that consumer's responsibility to ``ack`` or ``reject`` messages.

    Can be used directly, outside of standard baseplate context.

    """
    def __init__(self, worker, worker_thread):
        self.worker = worker
        self.worker_thread = worker_thread

    @classmethod
    def new(cls, connection, queues):
        """Create and initialize a consumer.

        :param kombu.Exchange exchange:
        :param list queues: List of :py:class:`kombu.queue.Queue` objects.

        """
        work_queue = queue.Queue()
        worker = _ConsumerWorker(connection, queues, work_queue)
        worker_thread = Thread(target=worker.run)
        worker_thread.name = "consumer message pump"
        worker_thread.daemon = True
        worker_thread.start()

        return cls(worker, worker_thread)

    def get_message(self):
        """Return a single message."""
        batch = self.get_batch(max_items=1, timeout=None)
        return batch[0]

    def get_batch(self, max_items, timeout):
        """Return a batch of messages.

        :param int max_items: The maximum batch size.
        :param int timeout: The maximum time to wait in seconds, or ``None``
            for no timeout.

        """
        if timeout == 0:
            block = False
        else:
            block = True

        batch = []
        retry_policy = RetryPolicy.new(attempts=max_items, budget=timeout)
        for time_remaining in retry_policy:
            item = self.worker.get_message(block=block, timeout=time_remaining)
            if item is None:
                break

            batch.append(item)

        return batch


class KombuConsumer(BaseKombuConsumer):
    """Consumer for use in baseplate.

    The :py:meth:`~baseplate.queue_consumer.KombuConsumer.get_message` and
    :py:meth:`~baseplate.queue_consumer.KombuConsumer.get_batch` methods will
    automatically record diagnostic information.

    """
    def get_message(self, server_span):
        """Return a single message.

        :param baseplate.core.ServerSpan server_span:

        """
        child_span = server_span.make_child("kombu.get_message")
        child_span.set_tag("kind", "consumer")

        with child_span:
            messages = BaseKombuConsumer.get_batch(
                self, max_items=1, timeout=None)
            message = messages[0]

            routing_key = message.delivery_info.get("routing_key", '')
            child_span.set_tag("routing_key", routing_key)

            consumer_tag = message.delivery_info.get("consumer_tag", '')
            child_span.set_tag("consumer_tag", consumer_tag)

            delivery_tag = message.delivery_info.get("delivery_tag", '')
            child_span.set_tag("delivery_tag", delivery_tag)

            exchange = message.delivery_info.get("exchange", '')
            child_span.set_tag("exchange", exchange)

            return message

    def get_batch(self, server_span, max_items, timeout):
        """Return a batch of messages.

        :param baseplate.core.ServerSpan server_span:
        :param int max_items: The maximum batch size.
        :param int timeout: The maximum time to wait in seconds, or ``None``
            for no timeout.

        """
        child_span = server_span.make_child("kombu.get_batch")
        child_span.set_tag("kind", "consumer")

        with child_span:
            messages = BaseKombuConsumer.get_batch(self, max_items, timeout)
            child_span.set_tag("message_count", len(messages))
            return messages
