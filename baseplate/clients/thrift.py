import contextlib
import inspect
import logging
import socket
import sys
import time
from collections import OrderedDict
from collections.abc import Iterator
from math import ceil
from typing import Any, Callable, Optional

from opentelemetry import trace
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.semconv.trace import MessageTypeValues, SpanAttributes
from opentelemetry.trace import status
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import Counter, Gauge, Histogram
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException, TException
from thrift.transport.TTransport import TTransportException

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config, metrics
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.lib.propagator_redditb3_thrift import RedditB3ThriftFormat
from baseplate.lib.retry import RetryPolicy
from baseplate.lib.thrift_pool import ThriftConnectionPool, thrift_pool_from_config
from baseplate.thrift.ttypes import Error, ErrorCode

logger = logging.getLogger(__name__)

propagator = CompositePropagator([RedditB3ThriftFormat(), TraceContextTextMapPropagator()])

PROM_NAMESPACE = "thrift_client"

PROM_COMMON_LABELS = [
    "thrift_method",
    "thrift_client_name",
]
REQUESTS_TOTAL_LABELS = [
    *PROM_COMMON_LABELS,
    "thrift_success",
    "thrift_exception_type",
    "thrift_baseplate_status",
    "thrift_baseplate_status_code",
]

REQUEST_LATENCY = Histogram(
    f"{PROM_NAMESPACE}_latency_seconds",
    "Latency of thrift client requests",
    [
        *PROM_COMMON_LABELS,
        "thrift_success",
    ],
    buckets=default_latency_buckets,
)

REQUESTS_TOTAL = Counter(
    f"{PROM_NAMESPACE}_requests_total",
    "Total number of outgoing requests",
    REQUESTS_TOTAL_LABELS,
)

ACTIVE_REQUESTS = Gauge(
    f"{PROM_NAMESPACE}_active_requests",
    "number of in-flight requests",
    PROM_COMMON_LABELS,
    multiprocess_mode="livesum",
)


class ThriftClient(config.Parser):
    """Configure a Thrift client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`baseplate.lib.thrift_pool.thrift_pool_from_config` for available
    configuration settings.

    :param client_cls: The class object of a Thrift-generated client class,
        e.g. ``YourService.Client``.

    """

    def __init__(self, client_cls: Any, **kwargs: Any):
        self.client_cls = client_cls
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> ContextFactory:
        pool = thrift_pool_from_config(raw_config, prefix=f"{key_path}.", **self.kwargs)
        return ThriftContextFactory(pool, self.client_cls)


class ThriftContextFactory(ContextFactory):
    """Thrift client pool context factory.

    This factory will attach a proxy object with the same interface as your
    thrift client to an attribute on the :py:class:`~baseplate.RequestContext`.
    When a thrift method is called on this proxy object, it will check out a
    connection from the connection pool and execute the RPC, automatically
    recording diagnostic information.

    :param pool: The connection pool.
    :param client_cls: The class object of a Thrift-generated client class,
        e.g. ``YourService.Client``.

    The proxy object has a ``retrying`` method which takes the same parameters
    as :py:meth:`RetryPolicy.new <baseplate.lib.retry.RetryPolicy.new>` and acts as
    a context manager. The context manager returns another proxy object where
    Thrift service method calls will be automatically retried with the
    specified retry policy when transient errors occur::

        with context.my_service.retrying(attempts=3) as svc:
            svc.some_method()

    """

    POOL_PREFIX = "thrift_client_pool"
    POOL_LABELS = ["thrift_pool"]

    max_connections_gauge = Gauge(
        f"{POOL_PREFIX}_max_size",
        "Maximum number of connections in this thrift pool before blocking",
        POOL_LABELS,
    )

    active_connections_gauge = Gauge(
        f"{POOL_PREFIX}_active_connections",
        "Number of connections currently in use in this thrift pool",
        POOL_LABELS,
    )

    def __init__(self, pool: ThriftConnectionPool, client_cls: Any):
        self.pool = pool
        self.client_cls = client_cls
        self.proxy_cls = type(
            "PooledClientProxy",
            (_PooledClientProxy,),
            {
                fn_name: _build_thrift_proxy_method(fn_name)
                for fn_name in _enumerate_service_methods(client_cls)
                if not (fn_name.startswith("__") and fn_name.endswith("__"))
            },
        )

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        pool_name = self.client_cls.__qualname__
        self.max_connections_gauge.labels(pool_name).set(self.pool.size)
        self.active_connections_gauge.labels(pool_name).set(self.pool.checkedout)
        batch.gauge("pool.size").replace(self.pool.size)
        batch.gauge("pool.in_use").replace(self.pool.checkedout)
        # it's hard to report "open_and_available" currently because we can't
        # distinguish easily between available connection slots that aren't
        # instantiated and ones that have actual open connections.

    def make_object_for_context(self, name: str, span: Span) -> "_PooledClientProxy":
        return self.proxy_cls(self.client_cls, self.pool, span, name)


def _enumerate_service_methods(client: Any) -> Iterator[str]:
    """Return an iterable of service methods from a generated Iface class."""
    ifaces_found = 0

    # python3 drops the concept of unbound methods, so they're just plain
    # functions and we have to account for that here. see:
    # https://stackoverflow.com/questions/17019949/why-is-there-a-difference-between-inspect-ismethod-and-inspect-isfunction-from-p  # noqa: E501
    def predicate(x: Any) -> bool:
        return inspect.isfunction(x) or inspect.ismethod(x)

    for base_cls in inspect.getmro(client):
        if base_cls.__name__ == "Iface":
            for name, _ in inspect.getmembers(base_cls, predicate):
                yield name
            ifaces_found += 1

    assert ifaces_found > 0, "class is not a thrift client; it has no Iface"


class _PooledClientProxy:
    """A proxy which acts like a thrift client but uses a connection pool."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        client_cls: Any,
        pool: ThriftConnectionPool,
        server_span: Span,
        namespace: str,
        retry_policy: Optional[RetryPolicy] = None,
    ):
        self.client_cls = client_cls
        self.pool = pool
        self.server_span = server_span
        self.namespace = namespace
        self.retry_policy = retry_policy or RetryPolicy.new(attempts=1)
        self.tracer = trace.get_tracer(__name__)

        self.otel_peer_name = None
        self.otel_peer_ip = None
        try:
            self.otel_peer_name = socket.getfqdn()
            self.otel_peer_ip = socket.gethostbyname(self.otel_peer_name)
        except socket.gaierror:
            logger.exception("Failed to retrieve local fqdn/pod name/pod IP for otel traces.")

    @contextlib.contextmanager
    def retrying(self, **policy: Any) -> Iterator["_PooledClientProxy"]:
        yield self.__class__(
            self.client_cls,
            self.pool,
            self.server_span,
            self.namespace,
            retry_policy=RetryPolicy.new(**policy),
        )


def _build_thrift_proxy_method(name: str) -> Callable[..., Any]:
    def _call_thrift_method(self: Any, *args: Any, **kwargs: Any) -> Any:
        last_error = None

        # this is technically incorrect, but we don't currently have a reliable way
        # of getting the name of the service being called, so relying on the name of
        # the client is the best we can do
        rpc_service = self.namespace
        rpc_method = name

        # RPC specific headers
        # 1.20 doc https://github.com/open-telemetry/opentelemetry-specification/blob/v1.20.0/specification/trace/semantic_conventions/rpc.md
        otel_attributes = {
            SpanAttributes.RPC_SYSTEM: "thrift",
            SpanAttributes.RPC_SERVICE: rpc_service,
            SpanAttributes.RPC_METHOD: rpc_method,
            SpanAttributes.NET_HOST_NAME: self.otel_peer_name,
            SpanAttributes.NET_HOST_IP: self.otel_peer_ip,
        }

        otelspan_name = f"{rpc_service}/{rpc_method}"
        trace_name = f"{self.namespace}.{name}"  # old bp.py span name

        for time_remaining in self.retry_policy:
            try:
                with (
                    self.pool.connection() as prot,
                    ACTIVE_REQUESTS.labels(
                        thrift_method=name, thrift_client_name=self.namespace
                    ).track_inprogress(),
                ):
                    start_time = time.perf_counter()

                    span = self.server_span.make_child(trace_name)
                    span.set_tag("slug", self.namespace)

                    client = self.client_cls(prot)
                    method = getattr(client, name)
                    span.set_tag("method", method.__name__)
                    span.start()

                    mutable_metadata: OrderedDict = OrderedDict()

                    pool_addr = self.pool.endpoint.address
                    if isinstance(pool_addr, str):
                        otel_attributes[SpanAttributes.NET_PEER_IP] = pool_addr
                    elif pool_addr is not None:
                        otel_attributes[SpanAttributes.NET_PEER_IP] = pool_addr.host
                        otel_attributes[SpanAttributes.NET_PEER_PORT] = pool_addr.port
                    if otel_attributes.get(SpanAttributes.NET_PEER_IP) in ["127.0.0.1", "::1"]:
                        otel_attributes[SpanAttributes.NET_PEER_NAME] = "localhost"
                    logger.debug(
                        "Will use the following otel span attributes. [span=%s, otel_attributes=%s]",  # noqa: E501
                        span,
                        otel_attributes,
                    )

                    with self.tracer.start_as_current_span(
                        otelspan_name,
                        kind=trace.SpanKind.CLIENT,
                        attributes=otel_attributes,
                    ) as otelspan:
                        try:
                            baseplate = span.baseplate
                            if baseplate:
                                service_name = baseplate.service_name
                                if service_name:
                                    prot.trans.set_header(b"User-Agent", service_name.encode())

                            # Inject all tracing headers into mutable_metadata and add as headers
                            propagator.inject(mutable_metadata)
                            for k, v in mutable_metadata.items():
                                prot.set_header(k.encode(), v.encode())

                            min_timeout = time_remaining
                            if self.pool.timeout:
                                if not min_timeout or self.pool.timeout < min_timeout:
                                    min_timeout = self.pool.timeout
                            if min_timeout and min_timeout > 0:
                                # min_timeout is in float seconds, we are
                                # converting to int milliseconds rounding up
                                # here.
                                prot.trans.set_header(
                                    b"Deadline-Budget", str(int(ceil(min_timeout * 1000))).encode()
                                )

                            try:
                                edge_context = span.context.raw_edge_context
                            except AttributeError:
                                edge_context = None

                            if edge_context:
                                prot.trans.set_header(b"Edge-Request", edge_context)

                            result = method(*args, **kwargs)
                        except TTransportException as exc:
                            # the connection failed for some reason, retry if able
                            span.finish(exc_info=sys.exc_info())
                            otelspan.set_status(status.Status(status.StatusCode.ERROR))
                            last_error = str(exc)
                            if exc.inner is not None:
                                last_error += f" ({exc.inner})"
                            # we need to raise all exceptions so that self.pool.connect() self-heals
                            raise
                        except (TApplicationException, TProtocolException):
                            # these are subclasses of TException but aren't ones that
                            # should be expected in the protocol. this is an error!
                            span.finish(exc_info=sys.exc_info())
                            otelspan.set_status(status.Status(status.StatusCode.ERROR))
                            raise
                        except Error as exc:
                            # a 5xx error is an unexpected exception but not 5xx are
                            # not.
                            if 500 <= exc.code < 600:
                                span.finish(exc_info=sys.exc_info())
                                otelspan.set_status(status.Status(status.StatusCode.ERROR))
                            else:
                                span.finish()
                            raise
                        except TException:
                            # this is an expected exception, as defined in the IDL
                            otelspan.set_status(status.Status(status.StatusCode.OK))
                            span.finish()
                            raise
                        except BaseException:
                            # something unexpected happened
                            span.finish(exc_info=sys.exc_info())
                            otelspan.set_status(status.Status(status.StatusCode.ERROR))
                            raise
                        else:
                            # a normal result
                            span.finish()
                            otelspan.set_status(status.Status(status.StatusCode.OK))
                            return result
                        finally:
                            event_attributes = {
                                SpanAttributes.MESSAGE_TYPE: MessageTypeValues.SENT.value,
                                # SpanAttributes.MESSAGE_ID: _,  # TODO if we want to
                                # SpanAttributes.MESSAGE_COMPRESSED_SIZE: _,  # TODO if we want to
                                # SpanAttributes.MESSAGE_UNCOMPRESSED_SIZE: _,  # TODO if we want to
                            }
                            otelspan.add_event(name="message", attributes=event_attributes)

                            thrift_success = "true"
                            exception_type = ""
                            baseplate_status = ""
                            baseplate_status_code = ""
                            exc_info = sys.exc_info()
                            if exc_info[0] is not None:
                                thrift_success = "false"
                                exception_type = exc_info[0].__name__
                            current_exc: Any = exc_info[1]
                            try:
                                # We want the following code to execute
                                # whenever the service raises an instance of
                                # Baseplate's `Error` class. Unfortunately, we
                                # cannot just rely on `isinstance` to do what
                                # we want here because some services compile
                                # Baseplate's thrift file on their own and
                                # import `Error` from that. When this is done,
                                # `isinstance` will always return `False` since
                                # it's technically a different class. To fix
                                # this, we optimistically try to access `code`
                                # on `current_exc` and just catch the
                                # `AttributeError` if the `code` attribute is
                                # not present. Note: if the error code was not
                                # originally defined in baseplate, or the name
                                # associated with the error was overriden, this
                                # cannot reflect that we will emit the status
                                # code in both cases but the status will be
                                # blank in the first case, and the baseplate
                                # name in the second

                                # Since this exception could be of any type, we
                                # may receive exceptions that have a `code`
                                # property that is actually not from
                                # Baseplate's `Error` class. In order to reduce
                                # (but not eliminate) the possibility of metric
                                # explosion, we validate it against the
                                # expected type for a proper Error code.
                                if isinstance(current_exc.code, int):
                                    baseplate_status_code = str(current_exc.code)
                                    baseplate_status = ErrorCode()._VALUES_TO_NAMES.get(
                                        current_exc.code, ""
                                    )
                            except AttributeError:
                                pass

                            REQUEST_LATENCY.labels(
                                thrift_method=name,
                                thrift_client_name=self.namespace,
                                thrift_success=thrift_success,
                            ).observe(time.perf_counter() - start_time)

                            REQUESTS_TOTAL.labels(
                                thrift_method=name,
                                thrift_client_name=self.namespace,
                                thrift_success=thrift_success,
                                thrift_exception_type=exception_type,
                                thrift_baseplate_status_code=baseplate_status_code,
                                thrift_baseplate_status=baseplate_status,
                            ).inc()

            except TTransportException:
                # swallow exception so we can retry on TTransportException (relies on the for loop)
                continue

        # this only happens if we exhaust the retry policy
        raise TTransportException(
            type=TTransportException.TIMED_OUT,
            message=f"retry policy exhausted while attempting {self.namespace}.{name}, last error was: {last_error}",  # noqa: E501
        )

    return _call_thrift_method
