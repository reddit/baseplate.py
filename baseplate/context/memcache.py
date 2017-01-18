from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pymemcache.client.base import PooledClient

from . import ContextFactory
from .. import config


def pool_from_config(app_config, prefix="memcache.", **kwargs):
    """Make a PooledClient from a configuration dictionary.

    The keys useful to :py:func:`pool_from_config` should be prefixed, e.g.
    ``memcache.endpoint``, ``memcache.max_pool_size``, etc. The ``prefix`` argument
    specifies the prefix used to filter keys. Each key is mapped to a
    corresponding keyword argument on the
    `PooledClient <https://pymemcache.readthedocs.io/en/latest/apidoc/pymemcache.client.base.html#pymemcache.client.base.PooledClient>`_
    constructor.

    Supported keys:

    * ``endpoint`` (required): a string representing a host and port to connect
        to memcached service, e.g. ``localhost:11211`` or ``127.0.0.1:11211``.
    * ``max_pool_size``: an integer for the maximum pool size to use, by default
        this is ``2147483648``.
    * ``connect_timeout``: a float representing seconds to wait for a connection to
        memcached server. Defaults to the underlying socket default timeout.
    * ``timeout``: a float representing seconds to wait for calls on the
        socket connected to memcache. Defaults to the underlying socket default
        timeout.

    """

    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "endpoint": config.Endpoint,
            "max_pool_size": config.Optional(config.Integer, default=None),
            "connect_timeout": config.Optional(config.Float, default=None),
            "timeout": config.Optional(config.Float, default=None),
        },
    })

    options = getattr(cfg, config_prefix)

    if options.max_pool_size is not None:
        kwargs.setdefault("max_pool_size", options.max_pool_size)
    if options.connect_timeout is not None:
        kwargs.setdefault("connect_timeout", options.connect_timeout)
    if options.timeout is not None:
        kwargs.setdefault("timeout", options.timeout)

    return PooledClient(options.endpoint.address, **kwargs)


class MemcacheContextFactory(ContextFactory):
    """Memcache client context factory.

    This factory will attach a
    :py:class:`~baseplate.context.memcache.MonitoredMemcacheConnection` to an
    attribute on the :term:`context object`. When memcache commands are executed
    via this connection object, they will use connections from the provided
    `PooledClient <https://pymemcache.readthedocs.io/en/latest/apidoc/pymemcache.client.base.html#pymemcache.client.base.PooledClient>`_
    and automatically record diagnostic information.

    :param pymemcache.client.base.PooledClient pooled_client: A pooled client.

    :returns: :py:class:`~baseplate.context.memcache.MonitoredMemcacheConnection`

    """
    def __init__(self, pooled_client):
        self.pooled_client = pooled_client

    def make_object_for_context(self, name, server_span):
        return MonitoredMemcacheConnection(name, server_span, self.pooled_client)


class MonitoredMemcacheConnection(PooledClient):
    """Memcache connection that collects diagnostic information.

    This connection acts like a
    `PooledClient <https://pymemcache.readthedocs.io/en/latest/apidoc/pymemcache.client.base.html#pymemcache.client.base.PooledClient>`_
    except that operations are wrapped with diagnostic collection. Some methods
    may not yet be wrapped with monitoring, please request assistance if any
    needed methods are not being monitored.

    """

    def __init__(self, context_name, server_span, pooled_client):
        self.context_name = context_name
        self.server_span = server_span
        self.pooled_client = pooled_client

    #pylint: disable=no-self-argument
    def monitored(fn_name):
        def proxy_with_instrumentation(self, *a, **kw):
            trace_name = "{}.{}".format(self.context_name, fn_name)
            with self.server_span.make_child(trace_name):
                method = getattr(self.pooled_client, fn_name)
                return method(*a, **kw)
        return proxy_with_instrumentation

    close = monitored('close')
    set = monitored('set')
    set_many = monitored('set_many')
    replace = monitored('replace')
    append = monitored('append')
    prepend = monitored('prepend')
    cas = monitored('cas')
    get = monitored('get')
    get_many = monitored('get_many')
    gets_many = monitored('gets_many')
    delete = monitored('delete')
    delete_many = monitored('delete_many')
    add = monitored('add')
    incr = monitored('incr')
    decr = monitored('decr')
    touch = monitored('touch')
    stats = monitored('stats')
    flush_all = monitored('flush_all')
    quit = monitored('quit')
