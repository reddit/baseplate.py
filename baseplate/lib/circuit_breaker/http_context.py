from contextlib import contextmanager

from requests.exceptions import ConnectionError

from graphql_api.errors import GraphQLUpstreamHTTPRequestError
from graphql_api.http_adapter import HTTPRequestTimeout
from graphql_api.lib.circuit_breaker.observer import BreakerObserver


@contextmanager
def http_circuit_breaker(context, breaker):
    breaker_observer = BreakerObserver(context, breaker, on_tripped_fn=None)

    # why aren't we doing 
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


"""
Do we want to generalize this with:
* list of exceptions to catch and mark success=False (all other
  exceptions will just raise with success=True)
   * do we need something fancier (map of exceptions to fn returning
     bool) to check whether this exception should mark success=False?
* on_tripped_fn to run if check_state() raises. must this raise an
  exception?

"""