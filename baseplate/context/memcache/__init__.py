from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pymemcache.client.base import PooledClient

from ...context import ContextFactory
from ... import config


def pool_from_config(app_config, prefix="memcache.", serializer=None,
                     deserializer=None):
    """Make a PooledClient from a configuration dictionary.

    The keys useful to :py:func:`pool_from_config` should be prefixed, e.g.
    ``memcache.endpoint``, ``memcache.max_pool_size``, etc. The ``prefix``
    argument specifies the prefix used to filter keys. Each key is mapped to a
    corresponding keyword argument on the
    :py:class:`~pymemcache.client.base.PooledClient` constructor.

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

    :param dict app_config: the config dictionary
    :param str prefix: prefix for config keys
    :param callable serializer: function to serialize values to strings suitable
        for being stored in memcached. An example is
        :py:func:`~baseplate.context.memcache.lib.make_dump_and_compress_fn`.
    :param callable deserializer: function to convert strings returned from
        memcached to arbitrary objects, must be compatible with ``serializer``.
        An example is :py:func:`~baseplate.context.memcache.lib.decompress_and_load`.

    :returns: :py:class:`pymemcache.client.base.PooledClient`

    """

    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "endpoint": config.Endpoint,
            "max_pool_size": config.Optional(config.Integer, default=None),
            "connect_timeout": config.Optional(config.Float, default=None),
            "timeout": config.Optional(config.Float, default=None),
            "no_delay": config.Optional(config.Boolean, default=True),
        },
    })

    options = getattr(cfg, config_prefix)

    return PooledClient(
        server=options.endpoint.address,
        connect_timeout=options.connect_timeout,
        timeout=options.timeout,
        serializer=serializer,
        deserializer=deserializer,
        no_delay=options.no_delay,
        max_pool_size=options.max_pool_size,
    )


class MemcacheContextFactory(ContextFactory):
    """Memcache client context factory.

    This factory will attach a
    :py:class:`~baseplate.context.memcache.MonitoredMemcacheConnection` to an
    attribute on the :term:`context object`. When memcache commands are
    executed via this connection object, they will use connections from the
    provided :py:class:`~pymemcache.client.base.PooledClient` and automatically
    record diagnostic information.

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
    :py:class:`~pymemcache.client.base.PooledClient` except that operations are
    wrapped with diagnostic collection. Some methods may not yet be wrapped
    with monitoring. Please request assistance if any needed methods are not
    being monitored.

    """

    def __init__(self, context_name, server_span, pooled_client):
        self.context_name = context_name
        self.server_span = server_span
        self.pooled_client = pooled_client

    def close(self):
        with self._make_span("close"):
            return self.pooled_client.close()

    def set(self, key, value, expire=0, noreply=None):
        with self._make_span("set") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.set(
                key, value, expire=expire, noreply=noreply)

    def set_many(self, values, expire=0, noreply=None):
        with self._make_span("set_many") as span:
            span.set_tag("key_count", len(values))
            span.set_tag("keys", make_keys_str(values.keys()))
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.set_many(
                values, expire=expire, noreply=noreply)

    def replace(self, key, value, expire=0, noreply=None):
        with self._make_span("replace") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.replace(
                key, value, expire=expire, noreply=noreply)

    def append(self, key, value, expire=0, noreply=None):
        with self._make_span("append") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.append(
                key, value, expire=expire, noreply=noreply)

    def prepend(self, key, value, expire=0, noreply=None):
        with self._make_span("prepend") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.prepend(
                key, value, expire=expire, noreply=noreply)

    def cas(self, key, value, cas, expire=0, noreply=None):
        with self._make_span("cas") as span:
            span.set_tag("key", key)
            span.set_tag("cas", cas)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.cas(
                key, value, cas, expire=expire, noreply=noreply)

    def get(self, key, **kwargs):
        with self._make_span("get") as span:
            span.set_tag("key", key)
            return self.pooled_client.get(key, **kwargs)

    def get_many(self, keys):
        with self._make_span("get_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.get_many(keys)

    def gets(self, key, **kwargs):
        with self._make_span("gets") as span:
            span.set_tag("key", key)
            return self.pooled_client.gets(key, **kwargs)

    def gets_many(self, keys):
        with self._make_span("gets_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.gets_many(keys)

    def delete(self, key, noreply=None):
        with self._make_span("delete") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.delete(key, noreply=noreply)

    def delete_many(self, keys, noreply=None):
        with self._make_span("delete_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("noreply", noreply)
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.delete_many(keys, noreply=noreply)

    def add(self, key, value, expire=0, noreply=None):
        with self._make_span("add") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.add(key, value, expire=expire, noreply=noreply)

    def incr(self, key, value, noreply=False):
        with self._make_span("incr") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.incr(key, value, noreply=noreply)

    def decr(self, key, value, noreply=False):
        with self._make_span("decr") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.decr(key, value, noreply=noreply)

    def touch(self, key, expire=0, noreply=None):
        with self._make_span("touch") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.touch(key, expire=expire, noreply=noreply)

    def stats(self, *args):
        with self._make_span("stats"):
            return self.pooled_client.stats(*args)

    def flush_all(self, delay=0, noreply=None):
        with self._make_span("flush_all") as span:
            span.set_tag("delay", delay)
            span.set_tag("noreply", noreply)
            return self.pooled_client.flush_all(delay=delay, noreply=noreply)

    def quit(self):
        with self._make_span("quit"):
            return self.pooled_client.quit()

    def _make_span(self, method_name):
        """Get a child span of the current server span.

        The returned span is tagged with ``method_name`` and given a name
        that corresponds to the current context name and called method.

        """
        trace_name = "{}.{}".format(self.context_name, method_name)
        span = self.server_span.make_child(trace_name)
        span.set_tag("method", method_name)
        return span


def make_keys_str(keys):
    """Make a string representation of an iterable of keys.

    """
    keys_str = ",".join(x.decode("utf-8") if isinstance(x, bytes) else x for x in keys)
    if len(keys_str) > 100:
        return keys_str[:100] + "..."
    else:
        return keys_str
