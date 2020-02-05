import logging
import time

from typing import Any
from typing import Dict

import confluent_kafka

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import Producer

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config


logger = logging.getLogger(__name__)

TOPIC_SUFFIX = "_avro"
CHECK_INTERVAL_SECONDS = 0.001


class AvroProducerContextFactory(ContextFactory):
    def __init__(self, bootstrap_servers: str, schema_registry: str, acks: int):
        self.avro_producer = AvroProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "schema.registry.url": schema_registry,
                "acks": acks,
                # force sending 1 message at a time--don't wait for larger batches
                "queue.buffering.max.ms": 0,
                "max.in.flight": 1,
            }
        )

    def make_object_for_context(self, name: str, span: Span) -> "KafkaAvroProducer":
        return KafkaAvroProducer(name, span, self.avro_producer)


class KafkaAvroProducer:
    def __init__(self, name: str, span: Span, avro_producer: AvroProducer):
        self.name = name
        self.span = span
        self.avro_producer = avro_producer
        self.initialized = False

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

        if not self.initialized:
            # make a metadata request, otherwise we'll have to do this on our
            # first produce which can add up to 1s of extra latency
            with self.span.make_child(f"{self.name}.avro_producer.list_topics"):
                for _ in range(3):
                    try:
                        # list_topics() is a blocking call, so we have to use
                        # a very short timeout and retry after a short sleep.
                        self.avro_producer.list_topics(timeout=0.001)
                        break
                    except confluent_kafka.KafkaException:
                        time.sleep(0.001)
                else:
                    logger.info("list_topics() failed after 3 attempts")

            self.initialized = True

        serializer = self.avro_producer._serializer

        encode_trace_name = f"{self.name}.serializer.encode_record_with_schema_id"
        encode_span = self.span.make_child(encode_trace_name)

        with encode_span:
            encoded = serializer.encode_record_with_schema_id(schema_id, value)

        producer_trace_name = f"{self.name}.avro_producer.produce"
        producer_span = self.span.make_child(producer_trace_name)

        # Producer.produce() is asynchronous so we have to jump through some
        # hoops to get the result here at the call site.
        delivery_result: Dict[str, Any] = dict(complete=False, error=None, message=None)

        def on_delivery(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message) -> None:
            delivery_result["error"] = err
            delivery_result["message"] = msg
            delivery_result["complete"] = True

        with producer_span:
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

    """
    assert prefix.endswith(".")

    parser = config.SpecParser(
        {
            "bootstrap_servers": config.String,
            "schema_registry": config.String,
            "acks": config.Integer,
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    return AvroProducerContextFactory(
        options.bootstrap_servers, options.schema_registry, options.acks
    )
