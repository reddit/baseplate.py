from typing import Any
from typing import Optional

from kombu import Connection
from kombu import Exchange
from kombu.pools import Producers

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config


def connection_from_config(app_config: config.RawConfig, prefix: str, **kwargs: Any) -> Connection:
    """Make a Connection from a configuration dictionary.

    The keys useful to :py:func:`connection_from_config` should be prefixed,
    e.g. ``amqp.hostname`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys.  Each key is mapped to a corresponding keyword
    argument on the :py:class:`~kombu.connection.Connection` constructor.  Any
    keyword arguments given to this function will be passed through to the
    :py:class:`~kombu.connection.Connection` constructor. Keyword arguments
    take precedence over the configuration file.

    Supported keys:

    * ``hostname``
    * ``virtual_host``

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {"hostname": config.String, "virtual_host": config.Optional(config.String)}
    )
    options = parser.parse(prefix[:-1], app_config)
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


class KombuProducer(config.Parser):
    """Configure a Kombu producer.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`connection_from_config` and :py:func:`exchange_from_config`
    for available configurables.

    :param max_connections: The maximum number of connections.

    """

    def __init__(self, max_connections: Optional[int] = None):
        self.max_connections = max_connections

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "KombuProducerContextFactory":
        connection = connection_from_config(raw_config, prefix=f"{key_path}.")
        exchange = exchange_from_config(raw_config, prefix=f"{key_path}.")
        return KombuProducerContextFactory(
            connection, exchange, max_connections=self.max_connections
        )


class KombuProducerContextFactory(ContextFactory):
    """KombuProducer context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`kombu.Producer` to an attribute on the :term:`context object`.
    The :py:meth:`~baseplate.clients.kombu.KombuProducer.publish` method will
    automatically record diagnostic information.

    :param connection: A configured connection object.
    :param exchange: A configured exchange object
    :param max_connections: The maximum number of connections.

    """

    def __init__(
        self, connection: Connection, exchange: Exchange, max_connections: Optional[int] = None
    ):
        self.connection = connection
        self.exchange = exchange
        self.producers = Producers(limit=max_connections)

    def make_object_for_context(self, name: str, span: Span) -> "_KombuProducer":
        return _KombuProducer(name, span, self.connection, self.exchange, self.producers)


class _KombuProducer:
    def __init__(
        self,
        name: str,
        span: Span,
        connection: Connection,
        exchange: Exchange,
        producers: Producers,
    ):
        self.name = name
        self.span = span
        self.connection = connection
        self.exchange = exchange
        self.producers = producers

    def publish(self, body: Any, routing_key: Optional[str] = None, **kwargs: Any) -> Any:
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
