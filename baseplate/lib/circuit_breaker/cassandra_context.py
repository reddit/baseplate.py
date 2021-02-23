import sys

from contextlib import contextmanager

from cassandra.cluster import DriverException
from cassandra.cluster import NoHostAvailable

from baseplate.lib.circuit_breaker.errors import BreakerTrippedError
from baseplate.lib.circuit_breaker.observer import BreakerObserver


@contextmanager
def cassandra_circuit_breaker(context, breaker, on_tripped_fn=None):
    breaker_observer = BreakerObserver(context, breaker)

    try:
        breaker_observer.check_state()
    except BreakerTrippedError:
        # hmm, this is raising a different exception via
        # raise_graphql_server_error
        if on_tripped_fn:
            on_tripped_fn()

    success: bool = True
    try:
        yield
    except (NoHostAvailable, DriverException):
        # Errors of connection, timeout, etc.
        success = False
        raise
    finally:
        breaker_observer.register_attempt(success)

"""
How we would migrate graphql:

from graphql_api.errors import raise_graphql_server_error
from graphql_api.lib.delegations import PLATFORM_SLACK


def on_cassandra_breaker_tripped():
    raise_graphql_server_error(
        context, "Cassandra connection failure", upstream_exc_info=sys.exc_info(), owner=PLATFORM_SLACK
    )

breaker = context.cassandra_breaker.get_endpoint_breaker()
with cassandra_circuit_breaker(context, breaker, on_tripped_fn=on_cassandra_breaker_tripped):
    ...
"""
