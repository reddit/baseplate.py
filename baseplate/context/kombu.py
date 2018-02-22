from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kombu import Connection
from kombu import Exchange
from kombu.pools import Producers

from baseplate import config
from baseplate.context import ContextFactory


def connection_from_config(app_config, prefix, **kwargs):
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
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "hostname": config.String,
            "virtual_host": config.Optional(config.String),
        },
    })

    options = getattr(cfg, config_prefix)

    return Connection(
        hostname=options.hostname,
        virtual_host=options.virtual_host,
        **kwargs
    )


def exchange_from_config(app_config, prefix, **kwargs):
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
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "exchange_name": config.Optional(config.String),
            "exchange_type": config.String,
        },
    })

    options = getattr(cfg, config_prefix)

    return Exchange(
        name=options.exchange_name or '',
        type=options.exchange_type,
        **kwargs
    )


class KombuProducerContextFactory(ContextFactory):
    """KombuProducer context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`kombu.Producer` to an attribute on the :term:`context object`.
    The :py:meth:`~baseplate.context.kombu.KombuProducer.publish` method will
    automatically record diagnostic information.

    :param kombu.connection.Connection connection: A configured connection
        object.
    :param kombu.Exchange exchange: A configured exchange object
    :param int max_connections: The maximum number of connections.

    """
    def __init__(self, connection, exchange, max_connections=None):
        self.connection = connection
        self.exchange = exchange
        self.producers = Producers(limit=max_connections)

    def make_object_for_context(self, name, span):
        return KombuProducer(
            name, span, self.connection, self.exchange, self.producers)


class KombuProducer(object):
    def __init__(self, name, span, connection, exchange, producers):
        self.name = name
        self.span = span
        self.connection = connection
        self.exchange = exchange
        self.producers = producers

    def publish(self, body, routing_key=None, **kwargs):
        """Publish a message to the routing_key.

        :param str body: The message body.
        :param str routing_key: The routing key to publish to.

        See `Kombu Documentation`_ for other arguments.

        .. _Kombu Documentation:
            http://docs.celeryproject.org/projects/kombu/en/latest/reference/kombu.html#kombu.Producer.publish # noqa

        """
        trace_name = "{}.{}".format(self.name, "publish")
        child_span = self.span.make_child(trace_name)

        child_span.set_tag("kind", "producer")
        if routing_key:
            child_span.set_tag("message_bus.destination", routing_key)

        with child_span:
            producer_pool = self.producers[self.connection]
            with producer_pool.acquire(block=True) as producer:
                return producer.publish(
                    body=body,
                    routing_key=routing_key,
                    exchange=self.exchange,
                    **kwargs
                )
