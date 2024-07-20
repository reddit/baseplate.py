import logging

from contextlib import contextmanager
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Iterator
from typing import Tuple
from typing import Type

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.circuit_breaker.breaker import Breaker
from baseplate.lib.circuit_breaker.breaker import BreakerState
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError


logger = logging.getLogger(__name__)


class CircuitBreakerClientWrapperFactory(ContextFactory):
    """Provide an object combining a client and circuit breaker for use with the client.

    When attached to the baseplate `RequestContext` can be used like:

    ```
    breakable_exceptions = (...)  # exceptions indicating the service is unhealthy
    with context.breaker_wrapped_client.breaker_context("identifier", breakable_exceptions) as svc:
        svc.get_something()
    ```
    """

    def __init__(self, client_factory: ContextFactory, breaker_box: "CircuitBreakerBox"):
        self.client_factory = client_factory
        self.breaker_box = breaker_box

    def make_object_for_context(self, name: str, span: Span) -> Any:
        client = self.client_factory.make_object_for_context(name, span)

        return CircuitBreakerWrappedClient(span, self.breaker_box, client)


class CircuitBreakerWrappedClient:
    def __init__(self, span: Span, breaker_box: "CircuitBreakerBox", client: Any):
        self.span = span
        self.breaker_box = breaker_box
        self._client = client

    @property
    def client(self) -> Any:
        """Return the raw, undecorated client"""
        return self._client

    @contextmanager
    def breaker_context(
        self, operation: str, breakable_exceptions: Tuple[Type[Exception]]
    ) -> Iterator[Any]:
        """Get a context manager to perform client operations within.

        Yields the client to use within the breaker context.

        The context manager manages the Breaker's state and registers
        successes and failures.

        When the `Breaker` is in TRIPPED state all calls to this context
        manager will raise a `BreakerTrippedError` exception.

        :param operation: The operation name, used to get a specific `Breaker`.
        :param breakable_exceptions: Tuple of exceptions that count as failures
        """
        breaker = self.breaker_box.get(operation)

        if breaker.state == BreakerState.TRIPPED:
            logger.debug("Circuit breaker '%s' tripped; request failed fast", breaker.name)
            self.span.incr_tag(f"breakers.{breaker.name}.request.fail_fast")
            raise BreakerTrippedError()

        success: bool = True
        try:
            # yield to the application code that will use
            # the client covered by this breaker. if this
            # raises an exception we will catch it here.
            yield self._client
        except breakable_exceptions:
            # only known exceptions in `breakable_exceptions` should trigger
            # opening the circuit. the client call may raise exceptions that
            # are a meaningful response, like defined thrift IDL exceptions.
            success = False
            raise
        finally:
            prev = breaker.state
            breaker.register_attempt(success)
            final = breaker.state
            if prev != final:
                self.span.incr_tag(
                    f"breakers.{breaker.name}.state_change.{prev.value}.{final.value}"
                )


class CircuitBreakerBox:
    """Container for a client's `Breaker`s.

    Will lazily create `Breaker`s for each operation as needed. There
    is no global coordination across operations--each `Breaker` is
    isolated and does not consider the state or failure rates in other
    `Breaker`s.

    :param name: The base `Breaker` name. The full name is like "name.operation".
    :param samples: See `Breaker`
    :param trip_failure_ratio: See `Breaker`
    :param trip_for: See `Breaker`
    :param fuzz_ratio: See `Breaker`
    """

    def __init__(
        self,
        name: str,
        samples: int,
        trip_failure_ratio: float,
        trip_for: timedelta,
        fuzz_ratio: float,
    ):
        self.name = name
        self.samples = samples
        self.trip_failure_ratio = trip_failure_ratio
        self.trip_for = trip_for
        self.fuzz_ratio = fuzz_ratio
        self.breaker_box: Dict[str, Breaker] = {}

    def get(self, operation: str) -> Breaker:
        # lazy add breaker into breaker box
        if operation not in self.breaker_box:
            breaker = Breaker(
                name=f"{self.name}.{operation}",
                samples=self.samples,
                trip_failure_ratio=self.trip_failure_ratio,
                trip_for=self.trip_for,
                fuzz_ratio=self.fuzz_ratio,
            )
            self.breaker_box[operation] = breaker
        return self.breaker_box[operation]


def breaker_box_from_config(
    app_config: config.RawConfig, name: str, prefix: str = "breaker.",
) -> CircuitBreakerBox:
    """Make a CircuitBreakerBox from a configuration dictionary."""
    # TODO: fix default handling here. if these are not set
    # they will be None and passed through to the Breaker() constructor
    # which will override the defaults set in Breaker()
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "samples": config.Optional(config.Integer),
            "trip_failure_ratio": config.Optional(config.Float),
            "trip_for": config.Optional(config.Timespan),
            "fuzz_ratio": config.Optional(config.Float),
        }
    )
    options = parser.parse(prefix[:-1], app_config)
    return CircuitBreakerBox(
        name, options.samples, options.trip_failure_ratio, options.trip_for, options.fuzz_ratio
    )
