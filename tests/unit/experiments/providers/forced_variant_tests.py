from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from datetime import datetime, timedelta

from baseplate.experiments.providers import ISO_DATE_FMT, parse_experiment
from baseplate.experiments.providers.forced_variant import ForcedVariantExperiment

THIRTY_DAYS = timedelta(days=30)


class TestForcedVariantExperiment(unittest.TestCase):

    def test_unknown_type_returns_null_experiment(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "unknown",
            "expires": (datetime.utcnow() + THIRTY_DAYS).strftime(ISO_DATE_FMT),
            "experiment": {
                "id": 1,
                "name": "test",
                "variants": {
                    "control_1": 10,
                    "control_2": 10,
                }
            }
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))
        self.assertIs(experiment.variant(), None)
        self.assertFalse(experiment.should_log_bucketing())

    def test_global_override_returns_forced_variant(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "expires": (datetime.utcnow() + THIRTY_DAYS).strftime(ISO_DATE_FMT),
            "global_override": "foo",
            "experiment": {
                "id": 1,
                "name": "test",
                "variants": {
                    "control_1": 10,
                    "control_2": 10,
                }
            }
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_disable_returns_forced_variant(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "expires": (datetime.utcnow() + THIRTY_DAYS).strftime(ISO_DATE_FMT),
            "enabled": False,
            "experiment": {
                "id": 1,
                "name": "test",
                "variants": {
                    "control_1": 10,
                    "control_2": 10,
                }
            }
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_forced_variant(self):
        experiment = ForcedVariantExperiment("foo")
        self.assertIs(experiment.variant(), "foo")
        self.assertFalse(experiment.should_log_bucketing())

    def test_forced_variant_null(self):
        experiment = ForcedVariantExperiment(None)
        self.assertIs(experiment.variant(), None)
        self.assertFalse(experiment.should_log_bucketing())
