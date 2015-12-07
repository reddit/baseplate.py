from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import functools
import inspect

from . import ContextFactory


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

    """
    def __init__(self, pool, client_cls):
        self.pool = pool
        self.client_cls = client_cls

    def make_context(self, name, root_span):
        return PooledClientProxy(self.client_cls, self.pool, root_span, name)


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

    def __init__(self, client_cls, pool, root_span, namespace):
        self.client_cls = client_cls
        self.pool = pool
        self.root_span = root_span
        self.namespace = namespace

        for name in _enumerate_service_methods(client_cls):
            setattr(self, name, functools.partial(
                self._call_thrift_method, name))

    def _call_thrift_method(self, name, *args, **kwargs):
        trace_name = "{}.{}".format(self.namespace, name)

        with self.root_span.make_child(trace_name) as span:
            with self.pool.connection() as prot:
                prot.trans.set_header("Trace", str(span.trace_id))
                prot.trans.set_header("Parent", str(span.parent_id))
                prot.trans.set_header("Span", str(span.id))

                client = self.client_cls(prot)
                method = getattr(client, name)
                return method(*args, **kwargs)
