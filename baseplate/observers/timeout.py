from typing import Optional

import gevent

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate import SpanObserver
from baseplate.lib import config


# this deliberately inherits from BaseException rather than Exception, just
# like gevent.timeout.Timeout itself does, so that we don't get caught in
# "except Exception:" checks in the code being timed out.
class ServerTimeout(BaseException):
    def __init__(self, span_name: str, timeout_seconds: float, debug: bool):
        super().__init__()
        self.span_name = span_name
        self.timeout_seconds = timeout_seconds
        self.debug = debug


class TimeoutBaseplateObserver(BaseplateObserver):
    @classmethod
    def from_config(cls, app_config: config.RawConfig) -> "TimeoutBaseplateObserver":
        cfg = config.parse_config(
            app_config,
            {
                "server_timeout": {
                    "default": config.Optional(
                        config.TimespanOrInfinite,
                        default=config.InfiniteTimespan,
                    ),
                    "debug": config.Optional(config.Boolean, default=False),
                    "by_endpoint": config.DictOf(config.TimespanOrInfinite),
                }
            },
        )
        return cls(cfg.server_timeout)

    def __init__(self, timeout_config: config.ConfigNamespace):
        self.config = timeout_config

    def on_server_span_created(self, context: RequestContext, server_span: ServerSpan) -> None:
        timeout = self.config.by_endpoint.get(server_span.name, self.config.default)

        min_timeout = None
        if timeout is not config.InfiniteTimespan:
            min_timeout = timeout.total_seconds()

        try:
            deadline_budget = context.deadline_budget
            if deadline_budget:
                if not min_timeout or deadline_budget < min_timeout:
                    min_timeout = deadline_budget
        except AttributeError:
            # no deadline budget in request header
            pass

        if min_timeout:
            observer = TimeoutServerSpanObserver(server_span, min_timeout, self.config.debug)
            server_span.register(observer)


class TimeoutServerSpanObserver(SpanObserver):
    def __init__(self, span: ServerSpan, timeout_seconds: float, debug: bool):
        exception = ServerTimeout(span.name, timeout_seconds, debug)
        self.timeout = gevent.Timeout(timeout_seconds, exception)

    def on_start(self) -> None:
        self.timeout.start()

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.timeout.close()
