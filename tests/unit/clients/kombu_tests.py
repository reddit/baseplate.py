import pytest

from baseplate.clients.kombu import connection_from_config
from baseplate.clients.kombu import KombuThriftSerializer
from baseplate.lib.config import ConfigurationError
from baseplate.testing.lib.secrets import FakeSecretsStore
from baseplate.thrift.ttypes import Loid
from baseplate.thrift.ttypes import Request
from baseplate.thrift.ttypes import Session

from ... import does_not_raise


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
        return KombuThriftSerializer[Request](Request)

    @pytest.fixture
    def req(self):
        return Request(
            loid=Loid(id="t2_1", created_ms=100000000),
            session=Session(id="session-id"),
            authentication_token="auth-token",
        )

    @pytest.fixture
    def req_bytes(self):
        return b"\x0c\x00\x01\x0b\x00\x01\x00\x00\x00\x04t2_1\n\x00\x02\x00\x00\x00\x00\x05\xf5\xe1\x00\x00\x0c\x00\x02\x0b\x00\x01\x00\x00\x00\nsession-id\x00\x0b\x00\x03\x00\x00\x00\nauth-token\x00"  # noqa

    def test_name(self, serializer):
        assert serializer.name == "thrift-Request"

    def test_serialize(self, serializer, req, req_bytes):
        serialized = serializer.serialize(req)
        assert isinstance(serialized, bytes)
        assert serialized == req_bytes

    def test_deserialize(self, serializer, req, req_bytes):
        request = serializer.deserialize(req_bytes)
        assert isinstance(request, Request)
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
