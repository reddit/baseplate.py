import logging

from baseplate import Span
from baseplate.lib.circuit_breaker.breaker import Breaker
from baseplate.lib.circuit_breaker.breaker import BreakerState
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError

from contextlib import contextmanager
from typing import Callable
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import Type

from baseplate import RequestContext
from baseplate.lib.circuit_breaker.breaker import Breaker
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError


logger = logging.getLogger(__name__)


@contextmanager
def circuit_breaker(
    span: Span,
    breaker: Breaker,
    breakable_exceptions: Tuple[Type[Exception]],
    on_tripped_fn: Optional[Callable[[], None]] = None,
) -> Iterator[None]:
    """Get a context manager to perform client operations within.

    The context manager includes a `Breaker`.

    The context manager handles managing the Breaker's state
    and registering successes and failures.

    When the `Breaker` is in TRIPPED state all calls to this context
    manager will raise a `BreakerTrippedError` exception.

    :param span: The server span, typically `context.trace`
    :param breaker: The `Breaker` for this client attempt
    :param breakable_exceptions: Tuple of exceptions that count as failures
    :param on_tripped_fn: Optional method to be called when the breaker is in TRIPPED state 
    """
    breaker_observer = BreakerObserver(span, breaker)

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


class BreakerObserver:
    def __init__(self, span: Span, breaker: Breaker):
        # TODO: is this the correct API? shouldn't the context
        # factory pipe in the span instead?
        self.span = span
        self.breaker = breaker
        self.name = breaker.name

    def on_fast_failed_request(self) -> None:
        logger.debug(f"Circuit breaker '{self.name}' tripped; request failed fast")
        self.span.incr_tag(f"breakers.{self.name}.request.fail_fast")

    def on_state_change(self, prev: BreakerState, curr: BreakerState) -> None:
        self.span.incr_tag(
            f"breakers.{self.name}.state_change.{prev.value}.{curr.value}"
        )

    def register_attempt(self, success: bool) -> None:
        prev_state = self.breaker.state
        self.breaker.register_attempt(success)
        curr_state = self.breaker.state
        if prev_state != curr_state:
            self.on_state_change(prev_state, curr_state)

    def check_state(self) -> None:
        if self.breaker.state == BreakerState.TRIPPED:
            self.on_fast_failed_request()
            raise BreakerTrippedError()