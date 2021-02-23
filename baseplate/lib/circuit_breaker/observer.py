from baseplate.lib.circuit_breaker.breaker import BreakerState
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError

METRICS_PREFIX = "breakers"


class BreakerObserver:
    def __init__(self, context, breaker):
        self.context = context
        self.breaker = breaker
        self.name = breaker.name

    def on_fast_failed_request(self):
        self.context.logger.debug(f"Circuit breaker '{self.name}' tripped; request failed fast")
        self.context.trace.incr_tag(f"{METRICS_PREFIX}.{self.name}.request.fail_fast")

    def on_state_change(self, prev, curr):
        self.context.trace.incr_tag(
            f"{METRICS_PREFIX}.{self.name}.state_change.{prev.value}.{curr.value}"
        )

    def register_attempt(self, success):
        prev_state = self.breaker.state
        self.breaker.register_attempt(success)
        curr_state = self.breaker.state
        if prev_state != curr_state:
            self.on_state_change(prev_state, curr_state)

    def check_state(self):
        if self.breaker.state == BreakerState.TRIPPED:
            self.on_fast_failed_request()
            raise BreakerTrippedError()
