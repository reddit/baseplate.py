from collections.abc import Sequence
from typing import Optional

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind, TraceState
from opentelemetry.util.types import Attributes
from pyrate_limiter import Duration, Limiter, Rate


class RateLimited(Sampler):
    """
    The RateLimited Sampler will sample every request up to a specified requests per second.
    :param sampler: The parent opentelemetry sampler we are wrapping.
    :type sampler: opentelemetry.sdk.trace.sampling.Sampler
    :param rps: The number of requests per second we would like to sample up to.
    :type rps: int
    """

    def __init__(self, sampler: Sampler, rps: int):
        self.rps = rps
        rate = Rate(rps, Duration.SECOND)
        self.limiter = Limiter(rate, raise_when_fail=False)
        self.sampler = sampler

    def should_sample(
        self,
        parent_context: Optional[Context],
        trace_id: int,
        name: str,
        kind: Optional[SpanKind] = None,
        attributes: Attributes = None,
        links: Optional[Sequence[Link]] = None,
        trace_state: Optional[TraceState] = None,
    ) -> SamplingResult:
        res = self.sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )
        if res != SamplingResult(Decision.DROP) and self.limiter.try_acquire("ratelimit"):
            return res
        return SamplingResult(Decision.DROP)

    def get_description(self) -> str:
        return f"RateLimited(fixed rate sampling {self.rps})"
