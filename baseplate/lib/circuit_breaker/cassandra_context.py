import sys

from contextlib import contextmanager

from cassandra.cluster import DriverException
from cassandra.cluster import NoHostAvailable

from graphql_api.errors import raise_graphql_server_error
from graphql_api.lib.circuit_breaker.errors import BreakerTrippedError
from graphql_api.lib.circuit_breaker.observer import BreakerObserver
from graphql_api.lib.delegations import PLATFORM_SLACK


@contextmanager
def cassandra_circuit_breaker(context):
    breaker = context.cassandra_breaker.get_endpoint_breaker()
    breaker_observer = BreakerObserver(context, breaker)

    try:
        breaker_observer.check_state()
    except BreakerTrippedError:
        raise_graphql_server_error(
            context, "Cassandra connection failure", upstream_exc_info=sys.exc_info(), owner=PLATFORM_SLACK
        )

    success: bool = True
    try:
        yield
    except (NoHostAvailable, DriverException):
        # Errors of connection, timeout, etc.
        success = False
        raise
    finally:
        breaker_observer.register_attempt(success)
