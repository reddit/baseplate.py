from contextlib import contextmanager

from requests.exceptions import ConnectionError

from graphql_api.errors import GraphQLUpstreamHTTPRequestError
from graphql_api.http_adapter import HTTPRequestTimeout
from graphql_api.lib.circuit_breaker.observer import BreakerObserver


@contextmanager
def http_circuit_breaker(context, breaker):
    breaker_observer = BreakerObserver(context, breaker)
    breaker_observer.check_state()

    success: bool = True

    try:
        yield
    except (ConnectionError, HTTPRequestTimeout):
        # ConnectionError can be caused by DNS issues
        success = False
        raise
    except GraphQLUpstreamHTTPRequestError as e:
        if e.code >= 500:
            success = False
        raise
    except Exception:
        raise
    finally:
        breaker_observer.register_attempt(success)
