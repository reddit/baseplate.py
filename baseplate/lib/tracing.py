from collections.abc import Callable, Sequence  # pylint: disable=import-error
from typing import Any, Optional, Protocol

import gevent.pool
from opentelemetry import context
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


# Greenlet tracing utils
__Greenlet = gevent.Greenlet
__IMap = gevent.pool.IMap
__IMapUnordered = gevent.pool.IMapUnordered


class Runnable(Protocol):
    @property
    def bp_trace_context(self) -> Context: ...

    run: Callable


class TracingMixin:
    def __init__(self: Runnable, *args: Any, **kwargs: Any) -> None:
        self.bp_trace_context = context.get_current()
        super().__init__(*args, **kwargs)

    def run(self: Runnable) -> None:
        context.attach(self.bp_trace_context)
        super().run()


class TracedGreenlet(TracingMixin, gevent.Greenlet): ...


class TracedIMapUnordered(TracingMixin, gevent.pool.IMapUnordered): ...


class TracedIMap(TracedIMapUnordered, gevent.pool.IMap): ...


def patch_greenlet_tracing() -> None:
    if getattr(gevent, "__rddt_patch", False):
        return
    gevent.__rddt_patch = True
    _replace(TracedGreenlet, TracedIMap, TracedIMapUnordered)


def unpatch_greenlet_tracing() -> None:
    if not getattr(gevent, "__rddt_patch", False):
        return
    gevent.__rddt_patch = False

    _replace(__Greenlet, __IMap, __IMapUnordered)


def _replace(
    g_class: gevent.Greenlet,
    imap_class: gevent.pool.IMap,
    imap_unordered_class: gevent.pool.IMapUnordered,
) -> None:
    gevent.greenlet.Greenlet = g_class
    gevent.pool.Group.greenlet_class = g_class
    gevent.pool.Greenlet = g_class
    gevent._imap.Greenlet = g_class

    # replace gevent shortcuts
    gevent.Greenlet = gevent.greenlet.Greenlet
    gevent.spawn = gevent.greenlet.Greenlet.spawn
    gevent.spawn_later = gevent.greenlet.Greenlet.spawn_later

    # replace the original IMap classes with the new one
    gevent._imap.IMap = imap_class
    gevent.pool.IMap = imap_class
    gevent._imap.IMapUnordered = imap_unordered_class
    gevent.pool.IMapUnordered = imap_unordered_class
