from typing import Any

import baseplate

from baseplate.clients import ContextFactory
from baseplate.lib import config

from graphql_api.lib.circuit_breaker.breaker import Breaker


class CircuitBreakerFactory(ContextFactory):
    def __init__(self, name, cfg):
        self.breaker_box = CircuitBreakerBox(name.replace("_breaker", ""), cfg)

    def make_object_for_context(self, name: str, span: "baseplate.Span") -> Any:
        return self.breaker_box

    @staticmethod
    def get_breaker_cfg(app_config, default_prefix, cfg_prefix, cfg_spec):
        cfg = config.parse_config(app_config, {cfg_prefix: cfg_spec})
        breaker_cfg = getattr(cfg, cfg_prefix)
        default_cfg = config.parse_config(app_config, {default_prefix: cfg_spec})
        default_breaker_cfg = getattr(default_cfg, default_prefix)

        for k in cfg_spec:
            if getattr(breaker_cfg, k) is None:
                setattr(breaker_cfg, k, getattr(default_breaker_cfg, k))
        return breaker_cfg

    @classmethod
    def from_config(cls, name, app_config, default_prefix, cfg_prefix, cfg_spec):
        breaker_cfg = cls.get_breaker_cfg(app_config, default_prefix, cfg_prefix, cfg_spec)
        return cls(name, breaker_cfg)


class CircuitBreakerBox:
    def __init__(self, name, cfg):
        self.name = name
        self.cfg = cfg
        self.breaker_box = {}

    def get_endpoint_breaker(self, endpoint=None):
        if not endpoint:
            # service breaker
            endpoint = "service"

        # lazy add breaker into breaker box
        if endpoint not in self.breaker_box:
            breaker = Breaker(
                name=f"{self.name}.{endpoint}",
                samples=self.cfg.samples,
                trip_failure_ratio=self.cfg.trip_failure_ratio,
                trip_for=self.cfg.trip_for,
                fuzz_ratio=self.cfg.fuzz_ratio,
            )

            self.breaker_box[endpoint] = breaker
        return self.breaker_box[endpoint]
