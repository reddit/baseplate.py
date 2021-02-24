from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Optional

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.circuit_breaker.breaker import Breaker


class CircuitBreakerFactory(ContextFactory):
    def __init__(self, breaker_box: "CircuitBreakerBox"):
        self.breaker_box = breaker_box

    def make_object_for_context(self, name: str, span: Span) -> Any:
        return self.breaker_box


class CircuitBreakerBox:
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

    def get_endpoint_breaker(self, endpoint: Optional[str] = None) -> Breaker:
        if not endpoint:
            # service breaker
            endpoint = "service"

        # lazy add breaker into breaker box
        if endpoint not in self.breaker_box:
            breaker = Breaker(
                name=f"{self.name}.{endpoint}",
                samples=self.samples,
                trip_failure_ratio=self.trip_failure_ratio,
                trip_for=self.trip_for,
                fuzz_ratio=self.fuzz_ratio,
            )
            self.breaker_box[endpoint] = breaker
        return self.breaker_box[endpoint]


def breaker_box_from_config(
    app_config: config.RawConfig, name: str, prefix: str = "breaker.",
) -> CircuitBreakerBox:
    """Make a CircuitBreakerBox from a configuration dictionary.
    """
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
