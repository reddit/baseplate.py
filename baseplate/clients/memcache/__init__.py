from time import perf_counter
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from pymemcache.client.base import PooledClient

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.prometheus_metrics import default_latency_buckets


Serializer = Callable[[str, Any], Tuple[bytes, int]]
Deserializer = Callable[[str, bytes, int], Any]


def pool_from_config(
    app_config: config.RawConfig,
    prefix: str = "memcache.",
    serializer: Optional[Serializer] = None,
    deserializer: Optional[Deserializer] = None,
) -> PooledClient:
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
    * ``connect_timeout``: how long (as
        :py:func:`~baseplate.lib.config.Timespan`) to wait for a connection to
        memcached server. Defaults to the underlying socket default timeout.
    * ``timeout``: how long (as :py:func:`~baseplate.lib.config.Timespan`) to
        wait for calls on the socket connected to memcache. Defaults to the
        underlying socket default timeout.

    :param app_config: the raw application configuration
    :param prefix: prefix for configuration keys
    :param serializer: function to serialize values to strings suitable
        for being stored in memcached. An example is
        :py:func:`~baseplate.clients.memcache.lib.make_dump_and_compress_fn`.
    :param deserializer: function to convert strings returned from
        memcached to arbitrary objects, must be compatible with ``serializer``.
        An example is :py:func:`~baseplate.clients.memcache.lib.decompress_and_load`.

    :returns: :py:class:`pymemcache.client.base.PooledClient`

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "endpoint": config.Endpoint,
            "max_pool_size": config.Optional(config.Integer, default=None),
            "connect_timeout": config.Optional(config.TimespanWithLegacyFallback, default=None),
            "timeout": config.Optional(config.TimespanWithLegacyFallback, default=None),
            "no_delay": config.Optional(config.Boolean, default=True),
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    return PooledClient(
        server=options.endpoint.address,
        connect_timeout=options.connect_timeout and options.connect_timeout.total_seconds(),
        timeout=options.timeout and options.timeout.total_seconds(),
        serializer=serializer,
        deserializer=deserializer,
        no_delay=options.no_delay,
        max_pool_size=options.max_pool_size,
    )


class MemcacheClient(config.Parser):
    """Configure a Memcached client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`pool_from_config` for available configuration settings.

    :param serializer: function to serialize values to strings suitable
        for being stored in memcached. An example is
        :py:func:`~baseplate.clients.memcache.lib.make_dump_and_compress_fn`.
    :param deserializer: function to convert strings returned from
        memcached to arbitrary objects, must be compatible with ``serializer``.
        An example is :py:func:`~baseplate.clients.memcache.lib.decompress_and_load`.

    """

    def __init__(
        self, serializer: Optional[Serializer] = None, deserializer: Optional[Deserializer] = None
    ):
        self.serializer = serializer
        self.deserializer = deserializer

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "MemcacheContextFactory":
        pool = pool_from_config(
            raw_config,
            prefix=f"{key_path}.",
            serializer=self.serializer,
            deserializer=self.deserializer,
        )
        return MemcacheContextFactory(pool, key_path)


class MemcacheContextFactory(ContextFactory):
    """Memcache client context factory.

    This factory will attach a
    :py:class:`~baseplate.clients.memcache.MonitoredMemcacheConnection` to an
    attribute on the :py:class:`~baseplate.RequestContext`. When memcache
    commands are executed via this connection object, they will use connections
    from the provided :py:class:`~pymemcache.client.base.PooledClient` and
    automatically record diagnostic information.

    :param pooled_client: A pooled client.

    """

    PROM_PREFIX = "memcached_client_pool"
    PROM_LABELS = ["memcached_pool"]

    pool_size_gauge = Gauge(
        f"{PROM_PREFIX}_max_size",
        "Maximum number of connections allowed in this pool",
        PROM_LABELS,
    )

    used_connections_gauge = Gauge(
        f"{PROM_PREFIX}_active_connections",
        "Number of connections in this pool currently in use",
        PROM_LABELS,
    )

    free_connections_gauge = Gauge(
        f"{PROM_PREFIX}_idle_connections",
        "Number of free connections in this pool",
        PROM_LABELS,
    )

    def __init__(self, pooled_client: PooledClient, name: str = "default"):
        self.pooled_client = pooled_client
        self.name = name

    def report_memcache_runtime_metrics(self, batch: metrics.Client) -> None:
        pool = self.pooled_client.client_pool
        self.pool_size_gauge.labels(self.name).set(pool.max_size)
        self.free_connections_gauge.labels(self.name).set(len(pool.free))
        self.used_connections_gauge.labels(self.name).set(len(pool.used))

        batch.gauge("pool.in_use").replace(len(pool.used))
        batch.gauge("pool.open_and_available").replace(len(pool.free))
        batch.gauge("pool.size").replace(pool.max_size)

    def make_object_for_context(self, name: str, span: Span) -> "MonitoredMemcacheConnection":
        return MonitoredMemcacheConnection(name, span, self.pooled_client)


Key = Union[str, bytes]

PROM_NAMESPACE = "memcached"
LABELS_COMMON = [
    f"{PROM_NAMESPACE}_address",
    f"{PROM_NAMESPACE}_command",
]
LATENCY_SECONDS = Histogram(
    f"{PROM_NAMESPACE}_client_latency_seconds",
    "Latency histogram of outoing memcached requests",
    [*LABELS_COMMON, f"{PROM_NAMESPACE}_success"],
    buckets=default_latency_buckets,
)
REQUESTS_TOTAL = Counter(
    f"{PROM_NAMESPACE}_client_requests_total",
    "Total number of memcached requests",
    [*LABELS_COMMON, f"{PROM_NAMESPACE}_success"],
)
ACTIVE_REQUESTS = Gauge(
    f"{PROM_NAMESPACE}_client_active_requests",
    "Number of active requests",
    LABELS_COMMON,
)


def _prom_instrument(func: Any) -> Any:
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        labels_common = {
            f"{PROM_NAMESPACE}_address": str(self.pooled_client.server),
            f"{PROM_NAMESPACE}_command": func.__name__,
        }
        success = "true"
        start_time = perf_counter()

        try:
            with ACTIVE_REQUESTS.labels(**labels_common).track_inprogress():
                return func(self, *args, **kwargs)
        except:  # noqa
            success = "false"
            raise
        finally:
            REQUESTS_TOTAL.labels(**{**labels_common, f"{PROM_NAMESPACE}_success": success}).inc()
            LATENCY_SECONDS.labels(
                **{**labels_common, f"{PROM_NAMESPACE}_success": success}
            ).observe(perf_counter() - start_time)

    return wrapper


class MonitoredMemcacheConnection:
    """Memcache connection that collects diagnostic information.

    This connection acts like a
    :py:class:`~pymemcache.client.base.PooledClient` except that operations are
    wrapped with diagnostic collection. Some methods may not yet be wrapped
    with monitoring. Please request assistance if any needed methods are not
    being monitored.

    """

    def __init__(self, context_name: str, server_span: Span, pooled_client: PooledClient):
        self.context_name = context_name
        self.server_span = server_span
        self.pooled_client = pooled_client

    @_prom_instrument
    def close(self) -> None:
        with self._make_span("close"):
            return self.pooled_client.close()

    @_prom_instrument
    def set(self, key: Key, value: Any, expire: int = 0, noreply: Optional[bool] = None) -> bool:
        with self._make_span("set") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.set(key, value, expire=expire, noreply=noreply)

    @_prom_instrument
    def set_many(
        self, values: Dict[Key, Any], expire: int = 0, noreply: Optional[bool] = None
    ) -> List[str]:
        with self._make_span("set_many") as span:
            span.set_tag("key_count", len(values))
            span.set_tag("keys", make_keys_str(values.keys()))
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.set_many(values, expire=expire, noreply=noreply)

    @_prom_instrument
    def replace(
        self, key: Key, value: Any, expire: int = 0, noreply: Optional[bool] = None
    ) -> bool:
        with self._make_span("replace") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.replace(key, value, expire=expire, noreply=noreply)

    @_prom_instrument
    def append(self, key: Key, value: Any, expire: int = 0, noreply: Optional[bool] = None) -> bool:
        with self._make_span("append") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.append(key, value, expire=expire, noreply=noreply)

    @_prom_instrument
    def prepend(
        self, key: Key, value: Any, expire: int = 0, noreply: Optional[bool] = None
    ) -> bool:
        with self._make_span("prepend") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.prepend(key, value, expire=expire, noreply=noreply)

    @_prom_instrument
    def cas(
        self, key: Key, value: Any, cas: int, expire: int = 0, noreply: Optional[bool] = None
    ) -> Optional[bool]:
        with self._make_span("cas") as span:
            span.set_tag("key", key)
            span.set_tag("cas", cas)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.cas(key, value, cas, expire=expire, noreply=noreply)

    @_prom_instrument
    def get(self, key: Key, default: Any = None) -> Any:
        with self._make_span("get") as span:
            span.set_tag("key", key)
            kwargs = {}
            if default is not None:
                kwargs["default"] = default
            return self.pooled_client.get(key, **kwargs)

    @_prom_instrument
    def get_many(self, keys: Sequence[Key]) -> Dict[Key, Any]:
        with self._make_span("get_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.get_many(keys)

    @_prom_instrument
    def gets(
        self, key: Key, default: Optional[Any] = None, cas_default: Optional[Any] = None
    ) -> Tuple[Any, Any]:
        with self._make_span("gets") as span:
            span.set_tag("key", key)
            return self.pooled_client.gets(key, default=default, cas_default=cas_default)

    @_prom_instrument
    def gets_many(self, keys: Sequence[Key]) -> Dict[Key, Tuple[Any, Any]]:
        with self._make_span("gets_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.gets_many(keys)

    @_prom_instrument
    def delete(self, key: Key, noreply: Optional[bool] = None) -> bool:
        with self._make_span("delete") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.delete(key, noreply=noreply)

    @_prom_instrument
    def delete_many(self, keys: Sequence[Key], noreply: Optional[bool] = None) -> bool:
        with self._make_span("delete_many") as span:
            span.set_tag("key_count", len(keys))
            span.set_tag("noreply", noreply)
            span.set_tag("keys", make_keys_str(keys))
            return self.pooled_client.delete_many(keys, noreply=noreply)

    @_prom_instrument
    def add(self, key: Key, value: Any, expire: int = 0, noreply: Optional[bool] = None) -> bool:
        with self._make_span("add") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.add(key, value, expire=expire, noreply=noreply)

    @_prom_instrument
    def incr(self, key: Key, value: int, noreply: Optional[bool] = False) -> Optional[int]:
        with self._make_span("incr") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.incr(key, value, noreply=noreply)

    @_prom_instrument
    def decr(self, key: Key, value: int, noreply: Optional[bool] = False) -> Optional[int]:
        with self._make_span("decr") as span:
            span.set_tag("key", key)
            span.set_tag("noreply", noreply)
            return self.pooled_client.decr(key, value, noreply=noreply)

    @_prom_instrument
    def touch(self, key: Key, expire: int = 0, noreply: Optional[bool] = None) -> bool:
        with self._make_span("touch") as span:
            span.set_tag("key", key)
            span.set_tag("expire", expire)
            span.set_tag("noreply", noreply)
            return self.pooled_client.touch(key, expire=expire, noreply=noreply)

    @_prom_instrument
    def stats(self, *args: str) -> Dict[str, Any]:
        with self._make_span("stats"):
            return self.pooled_client.stats(*args)

    @_prom_instrument
    def flush_all(self, delay: int = 0, noreply: Optional[bool] = None) -> bool:
        with self._make_span("flush_all") as span:
            span.set_tag("delay", delay)
            span.set_tag("noreply", noreply)
            return self.pooled_client.flush_all(delay=delay, noreply=noreply)

    @_prom_instrument
    def quit(self) -> None:
        with self._make_span("quit"):
            return self.pooled_client.quit()

    def _make_span(self, method_name: str) -> Span:
        """Get a child span of the current server span.

        The returned span is tagged with ``method_name`` and given a name
        that corresponds to the current context name and called method.

        """
        trace_name = f"{self.context_name}.{method_name}"
        span = self.server_span.make_child(trace_name)
        span.set_tag("method", method_name)
        return span


def make_keys_str(keys: Iterable[Key]) -> str:
    """Make a string representation of an iterable of keys."""
    keys_str = ",".join(x.decode("utf-8") if isinstance(x, bytes) else x for x in keys)
    if len(keys_str) > 100:
        return keys_str[:100] + "..."
    return keys_str
