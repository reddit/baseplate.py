import json
import logging
import queue
import socket
import time

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

import confluent_kafka

from gevent.server import StreamServer

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate.server.queue_consumer import HealthcheckCallback
from baseplate.server.queue_consumer import make_simple_healthchecker
from baseplate.server.queue_consumer import MessageHandler
from baseplate.server.queue_consumer import PumpWorker
from baseplate.server.queue_consumer import QueueConsumerFactory


if TYPE_CHECKING:
    WorkQueue = queue.Queue[confluent_kafka.Message]  # pylint: disable=unsubscriptable-object
else:
    WorkQueue = queue.Queue


logger = logging.getLogger(__name__)


KafkaMessageDeserializer = Callable[[bytes], Any]
Handler = Callable[[RequestContext, Any, confluent_kafka.Message], None]


class KafkaConsumerWorker(PumpWorker):
    """Reads messages from the Kafka consumer and pumps them into the internal work_queue."""

    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        consumer: confluent_kafka.Consumer,
        work_queue: WorkQueue,
        batch_size: int = 1,
    ):
        self.baseplate = baseplate
        self.name = name
        self.consumer = consumer
        self.work_queue = work_queue
        self.batch_size = batch_size

        self.started = False
        self.stopped = False

    def run(self) -> None:
        logger.debug("Starting KafkaConsumerWorker.")
        self.started = True
        while not self.stopped:
            context = self.baseplate.make_context_object()
            with self.baseplate.make_server_span(context, f"{self.name}.pump") as span:
                with span.make_child("kafka.consume"):
                    messages = self.consumer.consume(num_messages=self.batch_size, timeout=0)

                if not messages:
                    logger.debug("waited 1s and received no messages, waiting again")
                    # we can't use timeouts on the kafka consumer methods
                    # because they call out to C code that ends up blocking
                    # the event loop. gevent will successfully yield control
                    # on this sleep call though.
                    # see https://tech.wayfair.com/2018/07/blocking-io-in-gunicorn-gevent-workers/
                    time.sleep(1)
                    continue

                with span.make_child("kafka.work_queue_put"):
                    for message in messages:
                        self.work_queue.put(message)

    def stop(self) -> None:
        # stop consuming, but leave the consumer instance intact. if we
        # close the consumer before the message handler is done it won't be able
        # to commit offsets
        logger.debug("Stopping KafkaConsumerWorker.")
        self.stopped = True


class KafkaMessageHandler(MessageHandler):
    """Reads messages from the internal work_queue and processes them."""

    def __init__(
        self,
        baseplate: Baseplate,
        name: str,
        handler_fn: Handler,
        message_unpack_fn: KafkaMessageDeserializer,
        on_success_fn: Optional[Handler] = None,
    ):
        self.baseplate = baseplate
        self.name = name
        self.handler_fn = handler_fn
        self.message_unpack_fn = message_unpack_fn
        self.on_success_fn = on_success_fn

    def handle(self, message: confluent_kafka.Message) -> None:
        context = self.baseplate.make_context_object()
        try:
            # We place the call to ``baseplate.make_server_span`` inside the
            # try/except block because we still want Baseplate to see and
            # handle the error (publish it to error reporting)
            with self.baseplate.make_server_span(context, f"{self.name}.handler") as span:
                error = message.error()
                if error:
                    # this isn't a real message, but is an error from Kafka
                    raise ValueError(f"KafkaError: {error.str()}")

                topic = message.topic()
                offset = message.offset()
                partition = message.partition()

                span.set_tag("kind", "consumer")
                span.set_tag("kafka.topic", topic)
                span.set_tag("kafka.key", message.key())
                span.set_tag("kafka.partition", partition)
                span.set_tag("kafka.offset", offset)
                span.set_tag("kafka.timestamp", message.timestamp())

                blob: bytes = message.value()

                try:
                    data = self.message_unpack_fn(blob)
                except Exception:
                    logger.error("skipping invalid message")
                    context.span.incr_tag(f"{self.name}.{topic}.invalid_message")
                    return

                try:
                    ingest_timestamp_ms = data["endpoint_timestamp"]
                    now_ms = int(time.time() * 1000)
                    message_latency = (now_ms - ingest_timestamp_ms) / 1000
                except (KeyError, TypeError):
                    # we can't guarantee that all publishers populate this field
                    # v2 events publishers (event collectors) do, but future
                    # kafka publishers may not
                    message_latency = None

                self.handler_fn(context, data, message)

                if self.on_success_fn:
                    self.on_success_fn(context, data, message)

                if message_latency is not None:
                    context.metrics.timer(f"{self.name}.{topic}.latency").send(message_latency)

                context.metrics.gauge(f"{self.name}.{topic}.offset.{partition}").replace(offset)
        except Exception:
            # let this exception crash the server so we'll stop processing messages
            # and won't commit offsets. when the server restarts it will get
            # this message again and try to process it.
            logger.exception(
                "Unhandled error while trying to process a message, terminating the server"
            )
            raise


class _BaseKafkaQueueConsumerFactory(QueueConsumerFactory):
    def __init__(
        self,
        name: str,
        baseplate: Baseplate,
        consumer: confluent_kafka.Consumer,
        handler_fn: Handler,
        kafka_consume_batch_size: int = 1,
        message_unpack_fn: KafkaMessageDeserializer = json.loads,
        health_check_fn: Optional[HealthcheckCallback] = None,
    ):
        """`_BaseKafkaQueueConsumerFactory` constructor.

        :param name: A name for your consumer process. Must look like "kafka_consumer.{group_name}"
        :param baseplate: The Baseplate set up for your consumer.
        :param consumer: An instance of :py:class:`~confluent_kafka.Consumer`.
        :param handler_fn: A `baseplate.frameworks.queue_consumer.kafka.Handler`
            function that will process an individual message.
        :param kafka_consume_batch_size: The number of messages the `KafkaConsumerWorker`
            reads from Kafka in each batch. Defaults to 1.
        :param message_unpack_fn: A function that takes one argument, the `bytes` message body
            and returns the message in the format the handler expects. Defaults to `json.loads`.
        :param health_check_fn: A `baseplate.server.queue_consumer.HealthcheckCallback`
            function that can be used to customize your health check.

        """
        self.name = name
        self.baseplate = baseplate
        self.consumer = consumer
        self.handler_fn = handler_fn
        self.kafka_consume_batch_size = kafka_consume_batch_size
        self.message_unpack_fn = message_unpack_fn
        self.health_check_fn = health_check_fn

    @classmethod
    def new(
        cls,
        name: str,
        baseplate: Baseplate,
        bootstrap_servers: str,
        group_id: str,
        topics: Sequence[str],
        handler_fn: Handler,
        kafka_consume_batch_size: int = 1,
        message_unpack_fn: KafkaMessageDeserializer = json.loads,
        health_check_fn: Optional[HealthcheckCallback] = None,
    ) -> "_BaseKafkaQueueConsumerFactory":
        """Return a new `_BaseKafkaQueueConsumerFactory`.

        This method will create the :py:class:`~confluent_kafka.Consumer` for you and is
        appropriate to use in most cases where you just need a basic consumer
        with sensible defaults.

        This method will also enforce naming standards for the Kafka consumer group
        and the baseplate server span.

        :param name: A name for your consumer process. Must look like "kafka_consumer.{group_name}"
        :param baseplate: The Baseplate set up for your consumer.
        :param bootstrap_servers: A comma delimited string of kafka brokers.
        :param group_id: The kafka consumer group id. Must look like "{service_name}.{group_name}"
            to help prevent collisions between services.
        :param topics: An iterable of kafka topics to consume from.
        :param handler_fn: A `baseplate.frameworks.queue_consumer.kafka.Handler`
            function that will process an individual message.
        :param kafka_consume_batch_size: The number of messages the `KafkaConsumerWorker`
            reads from Kafka in each batch. Defaults to 1.
        :param message_unpack_fn: A function that takes one argument, the `bytes` message body
            and returns the message in the format the handler expects. Defaults to `json.loads`.
        :param health_check_fn: A `baseplate.server.queue_consumer.HealthcheckCallback`
            function that can be used to customize your health check.

        """
        service_name, _, group_name = group_id.partition(".")
        assert service_name and group_name, "group_id must start with 'SERVICENAME.'"
        assert name == f"kafka_consumer.{group_name}"

        consumer = cls.make_kafka_consumer(bootstrap_servers, group_id, topics)

        return cls(
            name=name,
            baseplate=baseplate,
            consumer=consumer,
            handler_fn=handler_fn,
            kafka_consume_batch_size=kafka_consume_batch_size,
            message_unpack_fn=message_unpack_fn,
            health_check_fn=health_check_fn,
        )

    @classmethod
    def _consumer_config(cls) -> Dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def make_kafka_consumer(
        cls, bootstrap_servers: str, group_id: str, topics: Sequence[str]
    ) -> confluent_kafka.Consumer:
        consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            # reset the offset to the latest offset when no stored offset exists.
            # this means that when a new consumer group is created it will only
            # process new messages.
            "auto.offset.reset": "latest",
        }
        consumer_config.update(cls._consumer_config())
        consumer = confluent_kafka.Consumer(consumer_config)

        try:
            # we can allow a blocking timeout here because there is only one
            # consumer for the entire server
            metadata = consumer.list_topics(timeout=10)
        except confluent_kafka.KafkaException:
            logger.error("failed getting metadata from %s, exiting.", bootstrap_servers)
            raise

        all_topics = set(metadata.topics.keys())
        for topic in topics:
            assert (
                topic in all_topics
            ), f"topic '{topic}' does not exist. maybe it's misspelled or on a different kafka cluster?"

        # pylint: disable=unused-argument
        def log_assign(
            consumer: confluent_kafka.Consumer, partitions: List[confluent_kafka.TopicPartition]
        ) -> None:
            for topic_partition in partitions:
                logger.info("assigned %s/%s", topic_partition.topic, topic_partition.partition)

        # pylint: disable=unused-argument
        def log_revoke(
            consumer: confluent_kafka.Consumer, partitions: List[confluent_kafka.TopicPartition]
        ) -> None:
            for topic_partition in partitions:
                logger.info("revoked %s/%s", topic_partition.topic, topic_partition.partition)

        consumer.subscribe(topics, on_assign=log_assign, on_revoke=log_revoke)
        return consumer

    def build_pump_worker(self, work_queue: WorkQueue) -> KafkaConsumerWorker:
        return KafkaConsumerWorker(
            baseplate=self.baseplate,
            name=self.name,
            consumer=self.consumer,
            work_queue=work_queue,
            batch_size=self.kafka_consume_batch_size,
        )

    def build_message_handler(self) -> KafkaMessageHandler:
        return KafkaMessageHandler(
            self.baseplate, self.name, self.handler_fn, self.message_unpack_fn
        )

    def build_health_checker(self, listener: socket.socket) -> StreamServer:
        return make_simple_healthchecker(listener, callback=self.health_check_fn)


class InOrderConsumerFactory(_BaseKafkaQueueConsumerFactory):
    """Factory for running a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer` using Kafka.

    The `InOrderConsumerFactory` attempts to achieve in order, exactly once
    message processing.

    This will run a single `KafkaConsumerWorker` that reads messages from Kafka and
    puts them into an internal work queue. Then it will run a single `KafkaMessageHandler`
    that reads messages from the internal work queue, processes them with the
    `handler_fn`, and then commits each message's offset to Kafka.

    This one-at-a-time, in-order processing ensures that when a failure happens
    during processing we don't commit its offset (or the offset of any later
    messages) and that when the server restarts it will receive the failed
    message and attempt to process it again. Additionally, because each
    message's offset is committed immediately after processing we should
    never process a message more than once.

    For most cases where you just need a basic consumer with sensible defaults
    you can use `InOrderConsumerFactory.new`.

    If you need more control, you can create the :py:class:`~confluent_kafka.Consumer`
    yourself and use the constructor directly.

    """

    # we need to ensure that only a single message handler worker exists (max_concurrency = 1)
    # otherwise we could have out of order processing and mess up committing offsets to kafka!
    message_handler_count = 0

    @classmethod
    def _consumer_config(cls) -> Dict[str, Any]:
        return {
            # The consumer sends periodic heartbeats on a separate thread to
            # indicate its liveness to the broker. If no heartbeats are received by
            # the broker for a group member within the session timeout (because the
            # consumer and heartbeat thread have died), the broker
            # will remove the consumer from the group and trigger a rebalance.
            "heartbeat.interval.ms": 3000,
            "session.timeout.ms": 10000,
            # Maximum allowed time between calls to consume messages. If this
            # interval is exceeded the consumer is considered failed and the group
            # will rebalance in order to reassign the partitions to another consumer
            # group member.
            # Note: It is recommended to set enable.auto.offset.store=false for
            # long-time processing applications and then explicitly store offsets
            # after message processing, to make sure offsets are not auto-committed
            # prior to processing has finished.
            "max.poll.interval.ms": 300000,
            # disable offset autocommit, we'll manually commit.
            "enable.auto.commit": "false",
        }

    def build_message_handler(self) -> KafkaMessageHandler:
        assert self.message_handler_count == 0, "Can only run 1 message handler!"

        self.message_handler_count += 1

        # pylint: disable=unused-argument
        def commit_offset(
            context: RequestContext, data: Any, message: confluent_kafka.Message
        ) -> None:
            logger.debug(
                "committing topic %s partition %s offset %s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            with context.span.make_child("kafka.commit"):
                self.consumer.commit(message=message, asynchronous=False)

        return KafkaMessageHandler(
            self.baseplate,
            self.name,
            self.handler_fn,
            self.message_unpack_fn,
            # commit offset after each successful message handle()
            on_success_fn=commit_offset,
        )


class FastConsumerFactory(_BaseKafkaQueueConsumerFactory):
    """Factory for running a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer` using Kafka.

    The `FastConsumerFactory` prioritizes high throughput over exactly once
    message processing.

    This will run a single `KafkaConsumerWorker` that reads messages from Kafka and
    puts them into an internal work queue. Then it will run multiple `KafkaMessageHandler`s
    that read messages from the internal work queue, processes them with the
    `handler_fn`. The number of `KafkaMessageHandler` processes is controlled
    by the `max_concurrency` parameter in the `~baseplate.server.queue_consumer.QueueConsumerServer`
    configuration. Kafka partition offsets are automatically committed by the
    `confluent_kafka.Consumer` every 5 seconds, so any message that has been
    read by the `KafkaConsumerWorker` could be committed, regardless of whether
    it has been processed.

    This server should be able to achieve very high message processing throughput
    due to the multiple `KafkaMessageHandler` processes and less frequent, background
    partition offset commits. This does come at a price though: messages may be
    processed out of order, not at all, or multiple times. This is appropriate
    when processing throughput is important and it's acceptable to skip messages
    or process messages more than once (maybe there is ratelimiting in the
    handler or somewhere downstream).

    Messages processed out of order:
    Messages are added to the internal work queue in order, but one worker may
    finish processing a "later" message before another worker finishes
    processing an "earlier" message.

    Messages never processed:
    If the server crashes it may not have processed some messages that have already
    had their offsets automatically committed. When the server restarts it won't
    read those messages.

    Messages processed more than once:
    If the server crashes it may have processed some messages but not yet
    committed their offsets. When the server restarts it will reprocess those
    messages.

    For most cases where you just need a basic consumer with sensible defaults
    you can use `FastConsumerFactory.new`.

    If you need more control, you can create the :py:class:`~confluent_kafka.Consumer`
    yourself and use the constructor directly.

    """

    # pylint: disable=unused-argument
    @staticmethod
    def _commit_callback(
        err: confluent_kafka.KafkaError, topic_partition_list: List[confluent_kafka.TopicPartition]
    ) -> None:
        # called after automatic commits
        for topic_partition in topic_partition_list:
            topic = topic_partition.topic
            partition = topic_partition.partition
            offset = topic_partition.offset

            if topic_partition.error:
                logger.error(
                    "commit error topic %s partition %s offset %s", topic, partition, offset
                )
            elif offset == confluent_kafka.OFFSET_INVALID:
                # we receive offsets for all partitions. an offset value of
                # OFFSET_INVALID means that no offset was committed.
                pass
            else:
                logger.debug(
                    "commit success topic %s partition %s offset %s", topic, partition, offset
                )

    @classmethod
    def _consumer_config(cls) -> Dict[str, Any]:
        return {
            # The consumer sends periodic heartbeats on a separate thread to
            # indicate its liveness to the broker. If no heartbeats are received by
            # the broker for a group member within the session timeout (because the
            # consumer and heartbeat thread have died), the broker
            # will remove the consumer from the group and trigger a rebalance.
            "heartbeat.interval.ms": 3000,
            "session.timeout.ms": 10000,
            # Maximum allowed time between calls to consume messages. If this
            # interval is exceeded the consumer is considered failed and the group
            # will rebalance in order to reassign the partitions to another consumer
            # group member.
            # Note: It is recommended to set enable.auto.offset.store=false for
            # long-time processing applications and then explicitly store offsets
            # after message processing, to make sure offsets are not auto-committed
            # prior to processing has finished.
            "max.poll.interval.ms": 300000,
            # autocommit offsets every 5 seconds
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": 5000,
            "enable.auto.offset.store": "true",
            # register a commit callback so that we'll log commits
            "on_commit": cls._commit_callback,
        }
