import logging

from typing import Any
from typing import Dict
from typing import Optional

from confluent_kafka.avro import AvroProducer

from baseplate import _ExcInfo
from baseplate import Span
from baseplate import SpanObserver
from baseplate.clients import ContextFactory
from baseplate.lib import config

logger = logging.getLogger(__name__)


class AvroProducerContextFactory(ContextFactory):
    def __init__(self, bootstrap_servers: str, schema_registry: str, acks: int):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry = schema_registry
        self.acks = acks

    def make_object_for_context(self, name: str, span: Span) -> AvroProducer:
        avro_producer = KafkaAvroProducer(
            name,
            span,
            {
                "bootstrap.servers": self.bootstrap_servers,
                "schema.registry.url": self.schema_registry,
                "acks": self.acks,
            },
        )
        span.register(AvroProducerSpanObserver(avro_producer))
        return avro_producer


class KafkaAvroProducer:
    def __init__(self, name: str, span: Span, app_config: Dict[str, Any]):
        self.name = name
        self.span = span
        self.avro_producer = AvroProducer(app_config)

    def produce(self, topic: str, schema_id: int, value: Dict[str, Any]) -> None:
        """Encode `value` using the `schema_id` and produce the message to Kafka.

        :param topic: topic name
        :param schema_id: schema id associated with the schema that `value` conforms to. It will
                          be used to look up the schema from Schema Registry. This prevents callers
                          from using unregistered schemas
       :param value: object to serialize

        """
        serializer = self.avro_producer._serializer

        encode_trace_name = "{}.{}".format(self.name, "serializer.encode_record_with_schema_id")
        encode_span = self.span.make_child(encode_trace_name)

        with encode_span:
            buffer = serializer.encode_record_with_schema_id(schema_id, value)

        producer_trace_name = "{}.{}".format(self.name, "avro_producer.produce")
        producer_span = self.span.make_child(producer_trace_name)

        producer = super(AvroProducer, self.avro_producer)  # pylint: disable=bad-super-call;
        with producer_span:
            producer.produce(topic=topic, value=buffer)  # type: ignore


class AvroProducerSpanObserver(SpanObserver):
    """Automatically flush to kafka at the end of each request."""

    def __init__(self, kafka_avro_producer: KafkaAvroProducer):
        self.avro_producer = kafka_avro_producer.avro_producer

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.avro_producer.flush()


def avro_producer_from_config(
    app_config: config.RawConfig, prefix: str = "application_kafka."
) -> AvroProducerContextFactory:
    """Make an avro producer context factory from a configuration dictionary."""
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
