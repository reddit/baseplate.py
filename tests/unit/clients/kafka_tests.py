from unittest import mock

import pytest

from confluent_kafka.avro import AvroProducer  # noqa pylint: disable=unused-import
from confluent_kafka.avro import Producer  # noqa pylint: disable=unused-import

from baseplate import ServerSpan
from baseplate.clients.kafka import avro_producer_from_config
from baseplate.clients.kafka import AvroProducerContextFactory
from baseplate.clients.kafka import KafkaAvroProducer
from baseplate.lib.config import ConfigurationError

from ... import does_not_raise


@pytest.mark.parametrize(
    "_app_config,kwargs,expectation,expected",
    [
        ({}, {}, pytest.raises(ConfigurationError), {}),
        (
            {"application_kafka.bootstrap_servers": "127.0.0.1:9092"},
            {},
            pytest.raises(ConfigurationError),
            {},
        ),
        (
            {
                "application_kafka.bootstrap_servers": "127.0.0.1:9092",
                "application_kafka.schema.registry.url": "http://127.0.0.1:8081",
            },
            {},
            pytest.raises(ConfigurationError),
            {},
        ),
        (
            {
                "application_kafka.bootstrap_servers": "127.0.0.1:9092",
                "application_kafka.schema.registry.url": "http://127.0.0.1:8081",
            },
            {},
            pytest.raises(ConfigurationError),
            {},
        ),
        (
            {
                "application_kafka.bootstrap_servers": "127.0.0.1:9092",
                "application_kafka.schema_registry": "http://127.0.0.1:8081",
                "application_kafka.acks": "-1",
            },
            {},
            does_not_raise(),
            {},
        ),
    ],
)
def test_avro_producer_from_config(_app_config, kwargs, expectation, expected):
    with expectation:
        connection = avro_producer_from_config(_app_config, prefix="application_kafka.", **kwargs)
    for attr, value in expected.items():
        assert getattr(connection, attr) == value


@pytest.fixture
def span():
    sp = mock.MagicMock(spec=ServerSpan)
    sp.make_child().__enter__.return_value = mock.MagicMock()
    return sp


@pytest.fixture
def name():
    return "test_producer_avro"


@pytest.fixture
def bootstrap_servers():
    return "127.0.0.1:9092"


@pytest.fixture
def schema_registry():
    return "http://127.0.0.1:8081"


@pytest.fixture
def acks():
    return "-1"


@pytest.fixture
def topic():
    return "topic_1_avro"


@pytest.fixture
def schema_id():
    return 1


@pytest.fixture
def schema_value():
    return {"endpoint_timestamp": 1500079}


APP_CONFIG = {
    "application_kafka.bootstrap_servers": "127.0.0.1:9092",
    "application_kafka.schema_registry": "http://127.0.0.1:8081",
    "application_kafka.acks": "-1",
}


class TestAvroProducerContextFactory:
    def test_avro_producer_from_config(self, bootstrap_servers, schema_registry, acks):
        factory = avro_producer_from_config(APP_CONFIG, prefix="application_kafka.")
        assert isinstance(factory, AvroProducerContextFactory)
        assert isinstance(factory.avro_producer, AvroProducer)

    def test_avro_producer_from_config_bad_prefix(self, bootstrap_servers, schema_registry, acks):
        with pytest.raises(AssertionError):
            avro_producer_from_config(APP_CONFIG, prefix="app")

    def test_avro_producer_context_factory_init(
        self, name, span, bootstrap_servers, schema_registry, acks
    ):
        factory = AvroProducerContextFactory(bootstrap_servers, schema_registry, acks)
        kafka_avro_producer = factory.make_object_for_context(name, span)
        assert kafka_avro_producer.name == name
        assert kafka_avro_producer.span == span
        assert kafka_avro_producer.initialized is False
        assert isinstance(kafka_avro_producer, KafkaAvroProducer)
        assert isinstance(kafka_avro_producer.avro_producer, AvroProducer)

    @mock.patch.object(KafkaAvroProducer, "produce")
    def test_kafka_avro_producer_produce(
        self, name, span, bootstrap_servers, schema_registry, acks
    ):
        factory = AvroProducerContextFactory(bootstrap_servers, schema_registry, acks)
        _kafka_avro_producer = factory.make_object_for_context(name, span)
        _kafka_avro_producer.produce(
            topic="topic_1_avro", schema_id=1, value={"endpoint_timestamp": 1500079}
        )
        _kafka_avro_producer.produce.assert_called_once_with(
            topic="topic_1_avro", schema_id=1, value={"endpoint_timestamp": 1500079}
        )

    def test_kafka_avro_producer_produce_bad_topic(
        self, name, span, bootstrap_servers, schema_registry, acks
    ):

        factory = AvroProducerContextFactory(bootstrap_servers, schema_registry, acks)
        _kafka_avro_producer = factory.make_object_for_context(name, span)
        with pytest.raises(ValueError):
            _kafka_avro_producer.produce(
                topic="topic_1", schema_id=1, value={"endpoint_timestamp": 1500079}
            )

    @mock.patch("baseplate.clients.kafka.Producer")
    def test_producer_produce(self, mock_producer, topic, schema_id, schema_value, name, span):
        _avro_producer = mock.Mock()
        kafka_avro_producer = KafkaAvroProducer(name, span, _avro_producer)
        assert kafka_avro_producer.initialized is False

        mock_producer.produce.side_effect = ValueError
        with pytest.raises(ValueError):
            kafka_avro_producer.produce(topic, schema_id, schema_value)
        mock_producer.produce.assert_called_once()

        kafka_avro_producer.avro_producer._serializer.encode_record_with_schema_id.assert_called_once_with(
            schema_id, schema_value
        )
        kafka_avro_producer.avro_producer.list_topics.assert_called_once()
        assert kafka_avro_producer.initialized is True
        span.make_child.assert_has_calls(
            [
                mock.call(f"{kafka_avro_producer.name}.avro_producer.list_topics"),
                mock.call(f"{kafka_avro_producer.name}.serializer.encode_record_with_schema_id"),
                mock.call(f"{kafka_avro_producer.name}.avro_producer.produce"),
            ],
            any_order=True,
        )
