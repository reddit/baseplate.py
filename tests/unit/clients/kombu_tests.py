from unittest import mock

import pytest

from prometheus_client import REGISTRY

from baseplate.clients.kombu import _KombuProducer
from baseplate.clients.kombu import AMQP_PROCESSED_TOTAL
from baseplate.clients.kombu import AMQP_PROCESSING_TIME
from baseplate.clients.kombu import connection_from_config
from baseplate.clients.kombu import exchange_from_config
from baseplate.clients.kombu import KombuThriftSerializer
from baseplate.lib.config import ConfigurationError
from baseplate.testing.lib.secrets import FakeSecretsStore

from ... import does_not_raise
from ...integration.test_thrift.ttypes import ExampleStruct


def secrets():
    return FakeSecretsStore(
        {
            "secrets": {
                "secret/rabbitmq/account": {
                    "type": "credential",
                    "username": "spez",
                    "password": "hunter2",
                }
            },
        }
    )


@pytest.mark.parametrize(
    "app_config,kwargs,expectation,expected",
    [
        ({}, {}, pytest.raises(ConfigurationError), {}),
        ({"rabbitmq.virtual_host": "test"}, {}, pytest.raises(ConfigurationError), {}),
        ({"amqp.hostname": "amqp://rabbit.local:5672"}, {}, pytest.raises(ConfigurationError), {}),
        (
            {
                "rabbitmq.hostname": "amqp://rabbit.local:5672",
                "rabbitmq.credentials_secret": "secret/rabbitmq/account",
            },
            {},
            pytest.raises(ValueError),
            {},
        ),
        (
            {"rabbitmq.hostname": "amqp://rabbit.local:5672"},
            {},
            does_not_raise(),
            {"hostname": "rabbit.local", "userid": None, "password": None, "virtual_host": "/"},
        ),
        (
            {"rabbitmq.hostname": "amqp://rabbit.local:5672", "rabbitmq.virtual_host": "test"},
            {},
            does_not_raise(),
            {"hostname": "rabbit.local", "userid": None, "password": None, "virtual_host": "test"},
        ),
        (
            {"rabbitmq.hostname": "amqp://rabbit.local:5672", "rabbitmq.virtual_host": "test"},
            {"userid": "spez", "password": "hunter2"},
            does_not_raise(),
            {
                "hostname": "rabbit.local",
                "userid": "spez",
                "password": "hunter2",
                "virtual_host": "test",
            },
        ),
        (
            {
                "rabbitmq.hostname": "amqp://rabbit.local:5672",
                "rabbitmq.credentials_secret": "secret/rabbitmq/account",
            },
            {"secrets": secrets()},
            does_not_raise(),
            {
                "hostname": "rabbit.local",
                "userid": "spez",
                "password": "hunter2",
                "virtual_host": "/",
            },
        ),
    ],
)
def test_connection_from_config(app_config, kwargs, expectation, expected):
    with expectation:
        connection = connection_from_config(app_config, prefix="rabbitmq.", **kwargs)
    for attr, value in expected.items():
        assert getattr(connection, attr) == value


class TestKombuThriftSerializer:
    @pytest.fixture
    def serializer(self):
        return KombuThriftSerializer[ExampleStruct](ExampleStruct)

    @pytest.fixture
    def req(self):
        return ExampleStruct(string_field="foo", int_field=42)

    @pytest.fixture
    def req_bytes(self):
        return b"\x0b\x00\x01\x00\x00\x00\x03foo\n\x00\x02\x00\x00\x00\x00\x00\x00\x00*\x00"

    def test_name(self, serializer):
        assert serializer.name == "thrift-ExampleStruct"

    def test_serialize(self, serializer, req, req_bytes):
        serialized = serializer.serialize(req)
        assert isinstance(serialized, bytes)
        assert serialized == req_bytes

    def test_deserialize(self, serializer, req, req_bytes):
        request = serializer.deserialize(req_bytes)
        assert isinstance(request, ExampleStruct)
        assert request == req

    def test_serialize_errors(self, serializer):
        with pytest.raises(TypeError):
            serializer.serialize({"foo": "bar"})

    @pytest.mark.parametrize(
        "input,exc", [(None, EOFError), (b"foo", TypeError), ("foo", TypeError), (1, TypeError)]
    )
    def test_deserialize_errors(self, input, exc, serializer):
        with pytest.raises(exc):
            serializer.deserialize(input)


class Test_KombuProducer:
    def setup(self):
        AMQP_PROCESSING_TIME.clear()
        AMQP_PROCESSED_TOTAL.clear()

    @pytest.fixture
    def app_config(self):
        yield {
            "rabbitmq.hostname": "amqp://rabbit.local:5672",
            "rabbitmq.exchange_type": "topic",
            "rabbitmq.exchange_name": "test_name",
        }

    @pytest.fixture
    def prefix(self):
        yield "rabbitmq."

    @pytest.fixture
    def connection(self, app_config, prefix):
        yield connection_from_config(app_config, prefix=prefix)

    @pytest.fixture
    def exchange(self, app_config, prefix):
        yield exchange_from_config(app_config, prefix=prefix)

    @pytest.fixture
    def producer(self):
        yield mock.MagicMock()

    @pytest.fixture
    def producer_pool(self, producer):
        pp = mock.MagicMock()
        pp.acquire().__enter__.return_value = producer
        yield pp

    @pytest.fixture
    def producers(self, connection, producer_pool):
        p = mock.MagicMock()
        p.__getitem__.return_value = producer_pool
        yield p

    @pytest.fixture
    def span(self):
        yield mock.MagicMock()

    @pytest.fixture
    def kombu_producer(self, span, connection, exchange, producers):
        yield _KombuProducer("name", span, connection, exchange, producers)

    @pytest.fixture
    def expected_labels(self):
        yield {
            "amqp_address": "rabbit.local:5672",
            "amqp_virtual_host": "/",
            "amqp_exchange_type": "topic",
            "amqp_exchange_name": "test_name",
        }

    def test__on_success(self, kombu_producer, expected_labels):
        kombu_producer._on_success(1)
        expected_labels["amqp_success"] = "true"
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_PROCESSING_TIME._name}_bucket", {**expected_labels, **{"le": "+Inf"}}
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{AMQP_PROCESSED_TOTAL._name}_total", expected_labels) == 1
        )

    def test__on_error(self, kombu_producer, expected_labels):
        kombu_producer._on_error(1)
        expected_labels["amqp_success"] = "false"
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_PROCESSING_TIME._name}_bucket", {**expected_labels, **{"le": "+Inf"}}
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{AMQP_PROCESSED_TOTAL._name}_total", expected_labels) == 1
        )

    def test_publish_prom_exc(self, kombu_producer, expected_labels, producer):
        producer.publish.side_effect = Exception("Any error")

        with pytest.raises(Exception, match=r"^Any error$"):
            kombu_producer.publish(body="doesn't matter")
        expected_labels["amqp_success"] = "false"
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_PROCESSING_TIME._name}_bucket", {**expected_labels, **{"le": "+Inf"}}
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{AMQP_PROCESSED_TOTAL._name}_total", expected_labels) == 1
        )
