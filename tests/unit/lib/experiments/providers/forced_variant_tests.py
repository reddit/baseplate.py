import time
import unittest

from datetime import datetime
from datetime import timedelta

from baseplate.lib.experiments.providers import ISO_DATE_FMT
from baseplate.lib.experiments.providers import parse_experiment
from baseplate.lib.experiments.providers.forced_variant import ForcedVariantExperiment

THIRTY_DAYS = timedelta(days=30).total_seconds()


class TestForcedVariantExperiment(unittest.TestCase):
    def test_unknown_type_returns_null_experiment(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "unknown",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
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
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "global_override": "foo",
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_disable_returns_forced_variant(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": False,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_before_start_ts_returns_forced_variant(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() + THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS * 2,
            "enabled": True,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_after_stop_ts_returns_forced_variant(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS * 2,
            "stop_ts": time.time() - THIRTY_DAYS,
            "enabled": True,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_after_expires_returns_forced_variant(self):
        expires = (datetime.now() - timedelta(days=30)).strftime(ISO_DATE_FMT)
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "expires": expires,
            "enabled": True,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, ForcedVariantExperiment))

    def test_expires_ignores_start_ts(self):
        expires = (datetime.now() + timedelta(days=30)).strftime(ISO_DATE_FMT)
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() + THIRTY_DAYS,
            "expires": expires,
            "enabled": True,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertFalse(isinstance(experiment, ForcedVariantExperiment))

    def test_start_ts_and_stop_ts_ignore_expires(self):
        expires = (datetime.now() - timedelta(days=30)).strftime(ISO_DATE_FMT)
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "expires": expires,
            "enabled": True,
            "experiment": {"id": 1, "name": "test", "variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertFalse(isinstance(experiment, ForcedVariantExperiment))

    def test_forced_variant(self):
        experiment = ForcedVariantExperiment("foo")
        self.assertIs(experiment.variant(), "foo")
        self.assertFalse(experiment.should_log_bucketing())

    def test_forced_variant_null(self):
        experiment = ForcedVariantExperiment(None)
        self.assertIs(experiment.variant(), None)
        self.assertFalse(experiment.should_log_bucketing())
