import logging
import time

from concurrent.futures import wait
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import confluent_kafka

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import Producer
from gevent.threadpool import ThreadPoolExecutor

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config

logger = logging.getLogger(__name__)

TOPIC_SUFFIX = "_avro"
CHECK_INTERVAL_SECONDS = 0.001


class AvroProducerContextFactory(ContextFactory):
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry: str,
        acks: int,
        thread_pool_max_size: int = 10,
        prefetch_schema_ids: Optional[List[int]] = None,
    ):
        self.avro_producer = AvroProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "schema.registry.url": schema_registry,
                "acks": acks,
                # force sending 1 message at a time--don't wait for larger batches
                "queue.buffering.max.ms": 0,
                "max.in.flight": 1,
                # hardcode the defaults here because we reference them with our manual refreshing
                "topic.metadata.refresh.interval.ms": 300000,
                "metadata.max.age.ms": 900000,
            }
        )
        self.threadpool = ThreadPoolExecutor(thread_pool_max_size)
        self.initialize(self.threadpool, prefetch_schema_ids or [])
        self.last_initialized_at = time.time()

    def initialize(
        self, threadpool: ThreadPoolExecutor, prefetch_schema_ids: List[int], block: bool = True
    ) -> None:
        """Do some setup tasks to make sure the producer is ready to use."""
        futures = []

        # make a metadata request, otherwise we'll have to do this on our
        # first produce which can add up to 1s of extra latency.
        # list_topics() is a blocking call, so we have to use a separate
        # thread to avoid blocking the gevent event loop. We can't easily
        # make confluent_kafka gevent compatible here because it's wrapping
        # the C library librdkafka.
        futures.append(threadpool.submit(self.avro_producer.list_topics))

        # fetch and cache all the schemas we plan to use
        if prefetch_schema_ids:
            for schema_id in prefetch_schema_ids:
                futures.append(
                    threadpool.submit(
                        self.avro_producer._serializer.registry_client.get_by_id, schema_id
                    )
                )

        if block:
            _, not_done = wait(futures, timeout=1)
            if not_done:
                logger.info("AvroProducer timeout while initializing")

    def make_object_for_context(self, name: str, span: Span) -> "KafkaAvroProducer":
        # When initializing the producer we request the metadata by calling list_topics()
        # Metadata is automatically refreshed every 5 minutes (topic.metadata.refresh.interval.ms)
        # and expires from cache after 15 minutes (metadata.max.age.ms). We should attempt to
        # refresh manually before it expires (after 10 minutes).
        METADATA_REFRESH_INTERVAL = 600

        now = time.time()
        if now - self.last_initialized_at > METADATA_REFRESH_INTERVAL:
            self.last_initialized_at = now
            self.initialize(threadpool=self.threadpool, prefetch_schema_ids=[], block=False)

        return KafkaAvroProducer(name, span, self.avro_producer)


class KafkaAvroProducer:
    def __init__(self, name: str, span: Span, avro_producer: AvroProducer):
        self.name = name
        self.span = span
        self.avro_producer = avro_producer

    def produce(self, topic: str, schema_id: int, value: Dict[str, Any]) -> None:
        """Encode `value` using the `schema_id` and produce the message to Kafka.

        :param topic: topic name that is of the format `<schema_name>_avro`
        :param schema_id: schema id associated with the schema that `value` conforms to. It will
                          be used to look up the schema from Schema Registry. This prevents callers
                          from using unregistered schemas
        :param value: object to serialize

        """

        if not topic.endswith(TOPIC_SUFFIX):
            raise ValueError("Avro Producers must publish to topics ending with '_avro'")

        serializer = self.avro_producer._serializer

        with self.span.make_child(f"{self.name}.serializer.encode_record_with_schema_id"):
            encoded = serializer.encode_record_with_schema_id(schema_id, value)

        # Producer.produce() is asynchronous so we have to jump through some
        # hoops to get the result here at the call site.
        delivery_result: Dict[str, Any] = dict(complete=False, error=None, message=None)

        def on_delivery(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message) -> None:
            delivery_result["error"] = err
            delivery_result["message"] = msg
            delivery_result["complete"] = True

        with self.span.make_child(f"{self.name}.avro_producer.produce"):
            # call the base class Producer.produce() method so we can do our own
            # interactions with schema registry to avoid a bug where the schema
            # can be re-registered.
            Producer.produce(
                self.avro_producer, topic=topic, value=encoded, on_delivery=on_delivery
            )

            self.avro_producer.flush(timeout=0)
            while not delivery_result["complete"]:
                # we can't use timeouts on the kafka consumer methods
                # because they call out to C code that ends up blocking
                # the event loop. gevent will successfully yield control
                # on this sleep call though.
                # see https://tech.wayfair.com/2018/07/blocking-io-in-gunicorn-gevent-workers/
                logger.debug(  # pylint: disable=logging-too-many-args
                    "waiting %s for pending message to flush", CHECK_INTERVAL_SECONDS
                )
                time.sleep(CHECK_INTERVAL_SECONDS)
                self.avro_producer.flush(timeout=0)

        if delivery_result["error"]:
            error = delivery_result["error"]
            raise ValueError(f"KafkaError: {error.str()}")

        message = delivery_result["message"]

        if not message:
            raise ValueError("Unexpected message delivery failure")

        logger.debug(  # pylint: disable=logging-too-many-args
            "message delivered (topic: %s, partition: %s, offset: %s)",
            message.topic(),
            message.partition(),
            message.offset(),
        )


def avro_producer_from_config(
    app_config: config.RawConfig, prefix: str
) -> AvroProducerContextFactory:
    """Make an avro producer context factory from a configuration dictionary.

    :param app_config: The raw configuration information.
    :param prefix: The name of the kafka cluster to publish messages to.

    The keys useful to :py:func:`avro_producer_from_config` should be prefixed, e.g.
    ``kafka_cluster_name.bootstrap_servers`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys. It should map to the name of the kafka cluster
    that will be published too.

    Supported keys:

    * ``bootstrap_servers`` (required): comma delimited list of kafka brokers to
      try connecting to, including the port, kafka-01.data.net:9092.
    * ``schema_registry``: url to connect to the schema-registry on, http://cp-schema-registry.data.net:80.
    * ``threadpool_size`` (optional): metadata is requested from kafka in it's own thread, option to specify the size of that threadpool.
    * ``prefetch_schema_ids`` (optional): to optimize initialization pass in list of int ids used by producers.

    """
    assert prefix.endswith(".")

    parser = config.SpecParser(
        {
            "bootstrap_servers": config.String,
            "schema_registry": config.String,
            "acks": config.Integer,
            "threadpool_size": config.Optional(config.Integer, default=10),
            "prefetch_schema_ids": config.Optional(config.TupleOf(config.Integer), default=()),
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    return AvroProducerContextFactory(
        options.bootstrap_servers,
        options.schema_registry,
        options.acks,
        options.threadpool_size,
        options.prefetch_schema_ids,
    )
