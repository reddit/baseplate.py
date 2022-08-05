import contextlib
import inspect
import sys
import time

from math import ceil
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Optional

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.lib.retry import RetryPolicy
from baseplate.lib.thrift_pool import thrift_pool_from_config
from baseplate.lib.thrift_pool import ThriftConnectionPool
from baseplate.thrift.ttypes import Error
from baseplate.thrift.ttypes import ErrorCode

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
        pool_name = self.client_cls.__name__
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
        trace_name = f"{self.namespace}.{name}"
        last_error = None

        for time_remaining in self.retry_policy:
            try:
                with self.pool.connection() as prot, ACTIVE_REQUESTS.labels(
                    thrift_method=name, thrift_client_name=self.namespace
                ).track_inprogress():
                    start_time = time.perf_counter()

                    span = self.server_span.make_child(trace_name)
                    span.set_tag("slug", self.namespace)

                    client = self.client_cls(prot)
                    method = getattr(client, name)
                    span.set_tag("method", method.__name__)
                    span.start()

                    try:
                        baseplate = span.baseplate
                        if baseplate:
                            service_name = baseplate.service_name
                            if service_name:
                                prot.trans.set_header(b"User-Agent", service_name.encode())

                        prot.trans.set_header(b"Trace", str(span.trace_id).encode())
                        prot.trans.set_header(b"Parent", str(span.parent_id).encode())
                        prot.trans.set_header(b"Span", str(span.id).encode())
                        if span.sampled is not None:
                            sampled = "1" if span.sampled else "0"
                            prot.trans.set_header(b"Sampled", sampled.encode())
                        if span.flags:
                            prot.trans.set_header(b"Flags", str(span.flags).encode())

                        min_timeout = time_remaining
                        if self.pool.timeout:
                            if not min_timeout or self.pool.timeout < min_timeout:
                                min_timeout = self.pool.timeout
                        if min_timeout and min_timeout > 0:
                            # min_timeout is in float seconds, we are converting to int milliseconds
                            # rounding up here.
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
                        last_error = str(exc)
                        if exc.inner is not None:
                            last_error += f" ({exc.inner})"
                        raise  # we need to raise all exceptions so that self.pool.connect() self-heals
                    except (TApplicationException, TProtocolException):
                        # these are subclasses of TException but aren't ones that
                        # should be expected in the protocol. this is an error!
                        span.finish(exc_info=sys.exc_info())
                        raise
                    except Error as exc:
                        # a 5xx error is an unexpected exception but not 5xx are
                        # not.
                        if 500 <= exc.code < 600:
                            span.finish(exc_info=sys.exc_info())
                        else:
                            span.finish()
                        raise
                    except TException:
                        # this is an expected exception, as defined in the IDL
                        span.finish()
                        raise
                    except:  # noqa: E722
                        # something unexpected happened
                        span.finish(exc_info=sys.exc_info())
                        raise
                    else:
                        # a normal result
                        span.finish()
                        return result
                    finally:
                        thrift_success = "true"
                        exception_type = ""
                        baseplate_status = ""
                        baseplate_status_code = ""
                        exc_info = sys.exc_info()
                        if exc_info[0] is not None:
                            thrift_success = "false"
                            exception_type = exc_info[0].__name__
                            current_exc = exc_info[1]
                            try:
                                # We want the following code to execute whenever the
                                # service raises an instance of Baseplate's `Error` class.
                                # Unfortunately, we cannot just rely on `isinstance` to do
                                # what we want here because some services compile
                                # Baseplate's thrift file on their own and import `Error`
                                # from that. When this is done, `isinstance` will always
                                # return `False` since it's technically a different class.
                                # To fix this, we optimistically try to access `code` on
                                # `current_exc` and just catch the `AttributeError` if the
                                # `code` attribute is not present.
                                # Note: if the error code was not originally defined in baseplate, or the
                                # name associated with the error was overriden, this cannot reflect that
                                # we will emit the status code in both cases
                                # but the status will be blank in the first case, and the baseplate name
                                # in the second
                                baseplate_status_code = current_exc.code  # type: ignore
                                baseplate_status = ErrorCode()._VALUES_TO_NAMES.get(current_exc.code, "")  # type: ignore
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
            message=f"retry policy exhausted while attempting {self.namespace}.{name}, last error was: {last_error}",
        )

    return _call_thrift_method
