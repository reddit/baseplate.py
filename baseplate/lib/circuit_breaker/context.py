from contextlib import contextmanager
from typing import Any
from typing import Callable
from typing import ContextManager
from typing import Optional

from baseplate import RequestContext
from baseplate.lib.circuit_breaker.breaker import Breaker
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError
from baseplate.lib.circuit_breaker.observer import BreakerObserver

"""
right now each remove service will generally have its own breaker
failures in any of its endpoints will be treated equally
should we have some more fine grained control? separate counters per
endpoint (if desired) plus tracking of overall error rate?
"""

@contextmanager
def circuit_breaker(
    context: RequestContext,
    breaker: Breaker,
    breakable_exceptions: tuple[Exception],
    on_tripped_fn: Optional[Callable[[Any], None]] = None,
) -> ContextManager:
    breaker_observer = BreakerObserver(context, breaker)

    try:
        breaker_observer.check_state()
    except BreakerTrippedError:
        # TODO: confirm that checking the breaker emits some metric
        if on_tripped_fn:
            # possibly raise a different exception here, or
            # do something before actually raising BreakerTrippedError
            on_tripped_fn()
        raise

    success: bool = True
    try:
        # yield to the application code that will use
        # the client covered by this breaker. if this
        # raises an exception we will catch it here.
        yield
    except breakable_exceptions:
        # only known exceptions in `breakable_exceptions` should trigger
        # opening the circuit. the client call may raise exceptions that
        # are a meaningful response, like defined thrift IDL exceptions.
        # TODO: should we invert this and take as an argument the known
        # good exceptions?
        success = False
        raise
        # TODO: should we do anything (metrics) for other exceptions?
    finally:
        breaker_observer.register_attempt(success)
