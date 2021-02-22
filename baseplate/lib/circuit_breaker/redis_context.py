from contextlib import contextmanager

from redis.exceptions import ConnectionError
from redis.exceptions import TimeoutError

from graphql_api.lib.circuit_breaker.observer import BreakerObserver


@contextmanager
def redis_circuit_breaker(context, breaker):
    breaker_observer = BreakerObserver(context, breaker)
    breaker_observer.check_state()

    success: bool = True
    try:
        yield
    except (ConnectionError, TimeoutError):
        success = False
        raise
    finally:
        breaker_observer.register_attempt(success)
