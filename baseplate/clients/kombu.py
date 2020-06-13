import abc

from typing import Any
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar

import kombu.serialization

from kombu import Connection
from kombu import Exchange
from kombu.pools import Producers
from thrift import TSerialization
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.protocol.TProtocol import TProtocolFactory

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.secrets import SecretsStore


T = TypeVar("T")


def connection_from_config(
    app_config: config.RawConfig, prefix: str, secrets: Optional[SecretsStore] = None, **kwargs: Any
) -> Connection:
    """Make a Connection from a configuration dictionary.

    The keys useful to :py:func:`connection_from_config` should be prefixed,
    e.g. ``amqp.hostname`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys.  Each key is mapped to a corresponding keyword
    argument on the :py:class:`~kombu.connection.Connection` constructor.  Any
    keyword arguments given to this function will be passed through to the
    :py:class:`~kombu.connection.Connection` constructor. Keyword arguments
    take precedence over the configuration file.

    Supported keys:

    * ``credentials_secret``
    * ``hostname``
    * ``virtual_host``

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "credentials_secret": config.Optional(config.String),
            "hostname": config.String,
            "virtual_host": config.Optional(config.String),
        }
    )
    options = parser.parse(prefix[:-1], app_config)
    if options.credentials_secret:
        if not secrets:
            raise ValueError("'secrets' is required if 'credentials_secret' is set")
        credentials = secrets.get_credentials(options.credentials_secret)
        kwargs.setdefault("userid", credentials.username)
        kwargs.setdefault("password", credentials.password)
    return Connection(hostname=options.hostname, virtual_host=options.virtual_host, **kwargs)


def exchange_from_config(app_config: config.RawConfig, prefix: str, **kwargs: Any) -> Exchange:
    """Make an Exchange from a configuration dictionary.

    The keys useful to :py:func:`exchange_from_config` should be prefixed,
    e.g. ``amqp.exchange_name`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys.  Each key is mapped to a corresponding keyword
    argument on the :py:class:`~kombu.Exchange` constructor.  Any keyword
    arguments given to this function will be passed through to the
    :py:class:`~kombu.Exchange` constructor. Keyword arguments take precedence
    over the configuration file.

    Supported keys:

    * ``exchange_name``
    * ``exchange_type``

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {"exchange_name": config.Optional(config.String), "exchange_type": config.String}
    )
    options = parser.parse(prefix[:-1], app_config)
    return Exchange(name=options.exchange_name or "", type=options.exchange_type, **kwargs)


class KombuSerializer(abc.ABC, Generic[T]):
    """Interface for wrapping non-built-in serializers for Kombu."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Return a unique name for the Serializer.

        This is used as the identifier for the serializer within Kombu so it
        should be unique.  It is also included in the "content-type" header so
        consumers are able to identify which serializer to use (in the format
        "application/x-{name}").
        """

    @abc.abstractmethod
    def serialize(self, obj: T) -> bytes:
        """Serialize the object into bytes for publishing."""

    @abc.abstractmethod
    def deserialize(self, message: bytes) -> T:
        """Deserialize the message bytes into an object for consuming."""


class KombuThriftSerializer(KombuSerializer[T]):  # pylint: disable=unsubscriptable-object
    """Thrift object serializer for Kombu."""

    def __init__(
        self,
        thrift_class: Type[T],
        protocol_factory: TProtocolFactory = TBinaryProtocolAcceleratedFactory(),
    ):
        self.thrift_class = thrift_class
        self.factory = protocol_factory

    @property
    def name(self) -> str:
        return f"thrift-{self.thrift_class.__name__}"

    def serialize(self, obj: T) -> bytes:
        if not isinstance(obj, self.thrift_class):
            raise TypeError(f"object to serialize must be of {self.thrift_class.__name__} type")
        return TSerialization.serialize(obj, self.factory)

    def deserialize(self, message: bytes) -> T:
        return TSerialization.deserialize(self.thrift_class(), message, self.factory)


def register_serializer(serializer: KombuSerializer) -> None:
    """Register `serializer` with the Kombu serialization registry.

    The serializer will be registered using `serializer.name` and will be sent
    to the message broker with the header "application/x-{serializer.name}".
    You need to call `register_serializer` before you can use a serializer for
    automatic serialization when publishing and deserializing when consuming.
    """
    kombu.serialization.register(
        serializer.name,
        serializer.serialize,
        serializer.deserialize,
        content_type=f"application/x-{serializer.name}",
        content_encoding="binary",
    )


class KombuProducer(config.Parser):
    """Configure a Kombu producer.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`connection_from_config` and :py:func:`exchange_from_config`
    for available configuration settings.

    :param max_connections: The maximum number of connections.
    :param serializer: A custom message serializer.
    :param secrets: `SecretsStore` for non-default connection credentials.
    """

    def __init__(
        self,
        max_connections: Optional[int] = None,
        serializer: Optional[KombuSerializer] = None,
        secrets: Optional[SecretsStore] = None,
    ):
        self.max_connections = max_connections
        self.serializer = serializer
        self.secrets = secrets

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "KombuProducerContextFactory":
        connection = connection_from_config(raw_config, prefix=f"{key_path}.", secrets=self.secrets)
        exchange = exchange_from_config(raw_config, prefix=f"{key_path}.")
        return KombuProducerContextFactory(
            connection, exchange, max_connections=self.max_connections, serializer=self.serializer
        )


class KombuProducerContextFactory(ContextFactory):
    """KombuProducer context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`kombu.Producer` to an attribute on the
    :py:class:`~baseplate.RequestContext`.  The
    :py:meth:`~baseplate.clients.kombu.KombuProducer.publish` method will
    automatically record diagnostic information.

    :param connection: A configured connection object.
    :param exchange: A configured exchange object
    :param max_connections: The maximum number of connections.

    """

    def __init__(
        self,
        connection: Connection,
        exchange: Exchange,
        max_connections: Optional[int] = None,
        serializer: Optional[KombuSerializer] = None,
    ):
        self.connection = connection
        self.exchange = exchange
        self.producers = Producers(limit=max_connections)
        self.serializer = serializer

    def make_object_for_context(self, name: str, span: Span) -> "_KombuProducer":
        return _KombuProducer(
            name, span, self.connection, self.exchange, self.producers, serializer=self.serializer
        )


class _KombuProducer:
    def __init__(
        self,
        name: str,
        span: Span,
        connection: Connection,
        exchange: Exchange,
        producers: Producers,
        serializer: Optional[KombuSerializer] = None,
    ):
        self.name = name
        self.span = span
        self.connection = connection
        self.exchange = exchange
        self.producers = producers
        self.serializer = serializer

    def publish(self, body: Any, routing_key: Optional[str] = None, **kwargs: Any) -> Any:
        if self.serializer:
            kwargs.setdefault("serializer", self.serializer.name)

        trace_name = "{}.{}".format(self.name, "publish")
        child_span = self.span.make_child(trace_name)

        child_span.set_tag("kind", "producer")
        if routing_key:
            child_span.set_tag("message_bus.destination", routing_key)

        with child_span:
            producer_pool = self.producers[self.connection]
            with producer_pool.acquire(block=True) as producer:
                return producer.publish(
                    body=body, routing_key=routing_key, exchange=self.exchange, **kwargs
                )
