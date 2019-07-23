from typing import Any
from typing import Optional

from baseplate.lib.experiments.providers.base import Experiment


class ForcedVariantExperiment(Experiment):
    """An experiment that always returns a specified variant.

    .. deprecated:: 0.27

    Should not log bucketing events to the event pipeline.  Note that
    ForcedVariantExperiments are not directly configured, rather they are
    used when an experiment is disabled or when "global_override" is set in
    the base config.
    """

    def __init__(self, variant: str):
        self._variant = variant

    def variant(self, **kwargs: Any) -> Optional[str]:
        return self._variant

    def should_log_bucketing(self) -> bool:
        return False
