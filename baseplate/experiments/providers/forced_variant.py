from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .base import Experiment


class ForcedVariantExperiment(Experiment):
    """An experiment that always returns a specified variant.

    .. deprecated:: 0.27

    Should not log bucketing events to the event pipeline.  Note that
    ForcedVariantExperiments are not directly configured, rather they are
    used when an experiment is disabled or when "global_override" is set in
    the base config.
    """

    def __init__(self, variant):
        self._variant = variant

    def variant(self, **kwargs):
        return self._variant

    def should_log_bucketing(self):
        return False
