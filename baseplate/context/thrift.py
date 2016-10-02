from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import functools
import inspect

from thrift.transport.TTransport import TTransportException

from . import ContextFactory
from ..retry import RetryPolicy


class ThriftContextFactory(ContextFactory):
    """Thrift client pool context factory.

    This factory will attach a proxy object with the same interface as your
    thrift client to an attribute on the :term:`context object`. When a thrift
    method is called on this proxy object, it will check out a connection from
    the connection pool and execute the RPC, automatically recording diagnostic
    information.

    :param baseplate.thrift_pool.ConnectionPool pool: The connection pool.
    :param client_cls: The class object of a Thrift-generated client class,
        e.g. ``YourService.Client``.

    The proxy object has a ``retrying`` method which takes the same parameters
    as :py:meth:`RetryPolicy.new <baseplate.retry.RetryPolicy.new>` and acts as
    a context manager. The context manager returns another proxy object where
    Thrift service method calls will be automatically retried with the
    specified retry policy when transient errors occur::

        with context.my_service.retrying(attempts=3) as svc:
            svc.some_method()

    """
    def __init__(self, pool, client_cls):
        self.pool = pool
        self.client_cls = client_cls

    def make_object_for_context(self, name, server_span):
        return PooledClientProxy(self.client_cls, self.pool, server_span, name)


def _enumerate_service_methods(client):
    """Return an iterable of service methods from a generated Iface class."""
    ifaces_found = 0

    # python3 drops the concept of unbound methods, so they're just plain
    # functions and we have to account for that here. see:
    # https://stackoverflow.com/questions/17019949/why-is-there-a-difference-between-inspect-ismethod-and-inspect-isfunction-from-p
    predicate = lambda x: inspect.isfunction(x) or inspect.ismethod(x)

    for base_cls in inspect.getmro(client):
        if base_cls.__name__ == "Iface":
            for name, _ in inspect.getmembers(base_cls, predicate):
                yield name
            ifaces_found += 1

    assert ifaces_found > 0, "class is not a thrift client; it has no Iface"


class PooledClientProxy(object):
    """A proxy which acts like a thrift client but uses a connection pool."""

    # pylint: disable=too-many-arguments
    def __init__(self, client_cls, pool, server_span, namespace, retry_policy=None):
        self.client_cls = client_cls
        self.pool = pool
        self.server_span = server_span
        self.namespace = namespace
        self.retry_policy = retry_policy or RetryPolicy.new(attempts=1)

        for name in _enumerate_service_methods(client_cls):
            setattr(self, name, functools.partial(
                self._call_thrift_method, name))

    @contextlib.contextmanager
    def retrying(self, **policy):
        yield PooledClientProxy(
            self.client_cls,
            self.pool,
            self.server_span,
            self.namespace,
            retry_policy=RetryPolicy.new(**policy),
        )

    def _call_thrift_method(self, name, *args, **kwargs):
        trace_name = "{}.{}".format(self.namespace, name)
        last_error = None

        for _ in self.retry_policy:
            try:
                with self.server_span.make_child(trace_name) as span:
                    with self.pool.connection() as prot:
                        prot.trans.set_header("Trace", str(span.trace_id))
                        prot.trans.set_header("Parent", str(span.parent_id))
                        prot.trans.set_header("Span", str(span.id))

                        client = self.client_cls(prot)
                        method = getattr(client, name)
                        return method(*args, **kwargs)
            except TTransportException as exc:
                last_error = exc
                continue

        raise TTransportException(
            type=TTransportException.TIMED_OUT,
            message="retry policy exhausted while attempting operation, "
                    "last error was: {}".format(last_error),
        )
