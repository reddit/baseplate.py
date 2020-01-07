import datetime
import time

from typing import Dict
from typing import Optional

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate import SpanObserver
from baseplate.lib import config


MAX_REQUEST_AGE = datetime.timedelta(minutes=1).total_seconds()


class ConcurrencyLimitReachedError(BaseException):
    def __init__(self, limit: int) -> None:
        self.limit = limit
        super().__init__(f"Too many concurrent requests (limit was {limit})")


class ConcurrencyObserver(BaseplateObserver):
    @classmethod
    def from_config(cls, app_config: config.RawConfig) -> "ConcurrencyObserver":
        cfg = config.parse_config(
            app_config, {"concurrency_limit": config.Optional(config.Integer)}
        )
        return cls(cfg.concurrency_limit)

    def __init__(self, limit: Optional[int]):
        self.live_requests: Dict[int, float] = {}
        self.limit = limit

    def cull_old_requests(self) -> None:
        threshold = time.time() - MAX_REQUEST_AGE
        stale_requests = [
            trace_id
            for trace_id, start_time in self.live_requests.items()
            if start_time < threshold
        ]
        for stale_request_id in stale_requests:
            self.live_requests.pop(stale_request_id, None)

    def on_server_span_created(self, context: RequestContext, server_span: ServerSpan) -> None:
        observer = ConcurrencyServerSpanObserver(self, server_span.trace_id, self.limit)
        server_span.register(observer)


class ConcurrencyServerSpanObserver(SpanObserver):
    def __init__(self, observer: ConcurrencyObserver, trace_id: int, limit: Optional[int]):
        self.observer = observer
        self.trace_id = trace_id
        self.limit = limit

    def on_start(self) -> None:
        if self.limit and len(self.observer.live_requests) >= self.limit:
            raise ConcurrencyLimitReachedError(self.limit)

        self.observer.live_requests[self.trace_id] = time.time()

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.observer.live_requests.pop(self.trace_id, None)
