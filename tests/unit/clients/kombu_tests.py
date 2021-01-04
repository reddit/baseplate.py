import pytest

from baseplate.clients.kombu import connection_from_config
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
