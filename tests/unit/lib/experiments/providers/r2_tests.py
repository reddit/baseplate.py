import collections
import math
import os
import time
import unittest

from datetime import timedelta
from unittest import mock

from baseplate import ServerSpan
from baseplate.lib.events import EventLogger
from baseplate.lib.experiments import ExperimentsContextFactory
from baseplate.lib.experiments.providers import parse_experiment
from baseplate.lib.experiments.providers.r2 import R2Experiment
from baseplate.lib.file_watcher import FileWatcher

THIRTY_DAYS = timedelta(days=30).total_seconds()


def get_users(num_users, logged_in=True):
    users = []
    for i in range(num_users):
        if logged_in:
            name = str(i)
        else:
            name = None
        users.append(dict(name=name, id="t2_%s" % str(i), logged_in=logged_in))
    return users


def generate_content(num_content, content_type):
    content = []

    if content_type == "subreddit":
        id_fmt = "t5_%s"
    elif content_type == "link":
        id_fmt = "t3_%s"
    elif content_type == "comment":
        id_fmt = "t1_%s"
    else:
        raise ValueError("Unknown content type: %s", content_type)

    for i in range(num_content):
        content.append(dict(id=id_fmt % i, type=content_type))

    return content


class TestR2Experiment(unittest.TestCase):
    def test_r2_type_returns_r2_experiment(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, R2Experiment))
        self.assertTrue(experiment.should_log_bucketing())
        self.assertEqual(experiment.version, "1")

    def test_no_version_allowed(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, R2Experiment))
        self.assertTrue(experiment.should_log_bucketing())
        self.assertIs(experiment.version, None)

    def test_calculate_bucket_value(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)
        experiment.num_buckets = 1000
        self.assertEqual(experiment._calculate_bucket("t2_1"), int(236))
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"seed": "test-seed", "variants": {"control_1": 10, "control_2": 10}},
        }
        seeded_experiment = parse_experiment(cfg)
        self.assertNotEqual(seeded_experiment.seed, experiment.seed)
        self.assertIsNot(seeded_experiment.seed, None)
        seeded_experiment.num_buckets = 1000
        self.assertEqual(seeded_experiment._calculate_bucket("t2_1"), int(595))

    @unittest.skipIf(
        "CI" not in os.environ, "test takes too long to run for normal local iteration"
    )
    def test_calculate_bucket(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"control_1": 10, "control_2": 10}},
        }
        experiment = parse_experiment(cfg)

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket.
        num_users = experiment.num_buckets * 1000
        fullnames = []
        for i in range(num_users):
            fullnames.append("t2_%s" % str(i))

        counter = collections.Counter()
        for fullname in fullnames:
            bucket = experiment._calculate_bucket(fullname)
            counter[bucket] += 1
            # Ensure bucketing is deterministic.
            self.assertEqual(bucket, experiment._calculate_bucket(fullname))

        for bucket in range(experiment.num_buckets):
            # We want an even distribution across buckets.
            expected = num_users / experiment.num_buckets
            actual = counter[bucket]
            # Calculating the percentage difference instead of looking at the
            # raw difference scales better as we change num_users.
            percent_equal = float(actual) / expected
            self.assertAlmostEqual(percent_equal, 1.0, delta=0.10, msg="bucket: %s" % bucket)

    @unittest.skipIf(
        "CI" not in os.environ, "test takes too long to run for normal local iteration"
    )
    def test_calculate_bucket_with_seed(self):
        cfg = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "variants": {"control_1": 10, "control_2": 10},
                "seed": "itscoldintheoffice",
            },
        }
        experiment = parse_experiment(cfg)

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket.
        num_users = experiment.num_buckets * 1000
        fullnames = []
        for i in range(num_users):
            fullnames.append("t2_%s" % str(i))

        counter = collections.Counter()
        bucketing_changed = False
        for fullname in fullnames:
            self.assertEqual(experiment.seed, "itscoldintheoffice")
            bucket1 = experiment._calculate_bucket(fullname)
            counter[bucket1] += 1
            # Ensure bucketing is deterministic.
            self.assertEqual(bucket1, experiment._calculate_bucket(fullname))

            current_seed = experiment.seed
            experiment.seed = "newstring"
            bucket2 = experiment._calculate_bucket(fullname)
            experiment.seed = current_seed
            # check that the bucketing changed at some point. Can't compare
            # bucket1 to bucket2 inline because sometimes the user will fall
            # into both buckets, and test will fail
            if bucket1 != bucket2:
                bucketing_changed = True

        self.assertTrue(bucketing_changed)

        for bucket in range(experiment.num_buckets):
            # We want an even distribution across buckets.
            expected = num_users / experiment.num_buckets
            actual = counter[bucket]
            # Calculating the percentage difference instead of looking at the
            # raw difference scales better as we change NUM_USERS.
            percent_equal = float(actual) / expected
            self.assertAlmostEqual(percent_equal, 1.0, delta=0.10, msg="bucket: %s" % bucket)

    def test_choose_variant(self):
        control_only = parse_experiment(
            {
                "id": 1,
                "name": "control_only",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"control_1": 10, "control_2": 10}},
            }
        )
        three_variants = parse_experiment(
            {
                "id": 1,
                "name": "three_variants",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {"remove_vote_counters": 5, "control_1": 10, "control_2": 5}
                },
            }
        )
        three_variants_more = parse_experiment(
            {
                "id": 1,
                "name": "three_variants_more",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {"remove_vote_counters": 15.6, "control_1": 10, "control_2": 20}
                },
            }
        )

        counters = collections.defaultdict(collections.Counter)
        for bucket in range(control_only.num_buckets):
            variant = control_only._choose_variant(bucket)
            if variant:
                counters[control_only.name][variant] += 1
            # Ensure variant-choosing is deterministic.
            self.assertEqual(variant, control_only._choose_variant(bucket))

            variant = three_variants._choose_variant(bucket)
            if variant:
                counters[three_variants.name][variant] += 1
            # Ensure variant-choosing is deterministic.
            self.assertEqual(variant, three_variants._choose_variant(bucket))

            previous_variant = variant
            variant = three_variants_more._choose_variant(bucket)
            if variant:
                counters[three_variants_more.name][variant] += 1
            # Ensure variant-choosing is deterministic.
            self.assertEqual(variant, three_variants_more._choose_variant(bucket))
            # If previously we had a variant, we should still have the same one
            # now.
            if previous_variant:
                self.assertEqual(variant, previous_variant)

        for experiment in (control_only, three_variants, three_variants_more):
            for variant, percentage in experiment.variants.items():
                count = counters[experiment.name][variant]
                scaled_percentage = float(count) / (experiment.num_buckets / 100)
                self.assertEqual(scaled_percentage, percentage)

        # Test boundary conditions around the maximum percentage allowed for
        # variants.
        fifty_fifty = parse_experiment(
            {
                "id": 1,
                "name": "fifty_fifty",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"control_1": 50, "control_2": 50}},
            }
        )
        almost_fifty_fifty = parse_experiment(
            {
                "id": 1,
                "name": "almost_fifty_fifty",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"control_1": 49, "control_2": 51}},
            }
        )
        for bucket in range(fifty_fifty.num_buckets):
            for experiment in (fifty_fifty, almost_fifty_fifty):
                variant = experiment._choose_variant(bucket)
                counters[experiment.name][variant] += 1

        count = counters[fifty_fifty.name]["control_1"]
        scaled_percentage = float(count) / (fifty_fifty.num_buckets / 100)
        self.assertEqual(scaled_percentage, 50)

        count = counters[fifty_fifty.name]["control_2"]
        scaled_percentage = float(count) / (fifty_fifty.num_buckets / 100)
        self.assertEqual(scaled_percentage, 50)

        count = counters[almost_fifty_fifty.name]["control_1"]
        scaled_percentage = float(count) / (almost_fifty_fifty.num_buckets / 100)
        self.assertEqual(scaled_percentage, 49)

        count = counters[almost_fifty_fifty.name]["control_2"]
        scaled_percentage = float(count) / (almost_fifty_fifty.num_buckets / 100)
        self.assertEqual(scaled_percentage, 50)

    def test_return_override_variant_without_bucket_val(self):
        experiment = parse_experiment(
            {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "overrides": {"user_name": {"gary": "active"}},
                    "variants": {"active": 10, "control_1": 10, "control_2": 20},
                },
            }
        )
        variant = experiment.variant(user_name="gary")
        self.assertEqual(variant, "active")
        variant = experiment.variant()
        self.assertEqual(variant, None)

    def test_non_string_override_value(self):
        experiment = parse_experiment(
            {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "overrides": {"logged_in": {True: "active"}},
                    "variants": {"active": 10, "control_1": 10, "control_2": 20},
                },
            }
        )
        variant = experiment.variant(logged_in=True)
        self.assertEqual(variant, "active")


@unittest.skipIf("CI" not in os.environ, "test takes too long to run for normal local iteration")
class TestSimulatedR2Experiments(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=EventLogger)
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.factory = ExperimentsContextFactory("path", self.event_logger)
        self.factory._filewatcher = self.mock_filewatcher
        self.now = time.time()

    def get_experiment_client(self, name):
        span = mock.MagicMock(spec=ServerSpan)
        span.context = None
        span.trace_id = "123456"
        return self.factory.make_object_for_context(name, span)

    def _simulate_experiment(self, config, static_vars, target_var, targets):
        num_experiments = len(targets)
        counter = collections.Counter()
        self.mock_filewatcher.get_data_and_mtime.return_value = {config["name"]: config}, self.now
        for target in targets:
            experiment_vars = {target_var: target}
            experiment_vars.update(static_vars)
            user = experiment_vars.pop("user")
            content = experiment_vars.pop("content")
            experiments = self.get_experiment_client("test")
            variant = experiments.variant(
                config["name"],
                user_id=user["id"],
                user_name=user["name"],
                logged_in=user["logged_in"],
                content_id=content["id"],
                content_type=content["type"],
                **experiment_vars,
            )
            if variant:
                counter[variant] += 1

        # this test will still probabilistically fail, but we can mitigate
        # the likeliness of that happening
        error_bar_percent = 100.0 / math.sqrt(num_experiments)
        experiment = parse_experiment(config)
        for variant, percent in experiment.variants.items():
            # Our actual percentage should be within our expected percent
            # (expressed as a part of 100 rather than a fraction of 1)
            # +- 1%.
            measured_percent = (float(counter[variant]) / num_experiments) * 100
            self.assertAlmostEqual(measured_percent, percent, delta=error_bar_percent)

    def do_user_experiment_simulation(self, users, config, content=None):
        content = content or dict(id=None, type=None)
        static_vars = {"content": content, "url_params": {}, "subreddit": None, "subdomain": None}
        return self._simulate_experiment(
            config=config, static_vars=static_vars, target_var="user", targets=users
        )

    def do_page_experiment_simulation(self, user, pages, config):
        static_vars = {"user": user, "url_params": {}, "subreddit": None, "subdomain": None}
        return self._simulate_experiment(
            config=config, static_vars=static_vars, target_var="content", targets=pages
        )

    def assert_no_user_experiment(self, users, config, content=None):
        content = content or dict(id=None, type=None)
        self.mock_filewatcher.get_data_and_mtime.return_value = {config["name"]: config}, self.now
        for user in users:
            experiments = self.get_experiment_client("test")
            self.assertIs(
                experiments.variant(
                    config["name"],
                    user_id=user["id"],
                    user_name=user["name"],
                    logged_in=user["logged_in"],
                    content_id=content["id"],
                    content_type=content["type"],
                ),
                None,
            )

    def assert_no_page_experiment(self, user, pages, config):
        self.mock_filewatcher.get_data_and_mtime.return_value = {config["name"]: config}, self.now
        for page in pages:
            experiments = self.get_experiment_client("test")
            self.assertIs(
                experiments.variant(
                    config["name"],
                    user_id=user["id"],
                    user_name=user["name"],
                    logged_in=user["logged_in"],
                    content_id=page["id"],
                    content_type=page["type"],
                ),
                None,
            )

    def assert_same_variant(self, users, config, expected, content=None, **kwargs):
        self.mock_filewatcher.get_data_and_mtime.return_value = {config["name"]: config}, self.now
        content = content or dict(id=None, type=None)
        for user in users:
            experiments = self.get_experiment_client("test")
            self.assertEqual(
                experiments.variant(
                    config["name"],
                    user_id=user["id"],
                    user_name=user["name"],
                    logged_in=user["logged_in"],
                    content_id=content["id"],
                    content_type=content["type"],
                    **kwargs,
                ),
                expected,
            )

    def test_experiment_overrides(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "overrides": {"url_features": {"test_larger": "larger", "test_smaller": "smaller"}},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.assert_same_variant(
            users=get_users(2000), config=config, expected="larger", url_features=["test_larger"]
        )
        self.assert_same_variant(
            users=get_users(2000),
            config=config,
            expected="larger",
            url_features=["test_larger", "test"],
        )
        self.assert_same_variant(
            users=get_users(2000),
            config=config,
            expected="smaller",
            url_features=["larger", "test_smaller"],
        )
        self.do_user_experiment_simulation(users=get_users(2000), config=config)

    def test_no_targeting_no_variant(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10}
            },
        }
        self.assert_no_user_experiment(users=get_users(2000), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=False), config=config)

    def test_loggedin_experiment(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_user_experiment_simulation(users=get_users(2000), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=False), config=config)

    def test_loggedin_experiment_explicit_enable(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_user_experiment_simulation(users=get_users(2000), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=False), config=config)

    def test_loggedin_experiment_explicit_disable(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": False,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.assert_no_user_experiment(users=get_users(2000), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=False), config=config)

    def test_loggedout_experiment(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_user_experiment_simulation(users=get_users(2000, logged_in=False), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=True), config=config)

    def test_loggedout_experiment_missing_loids(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        users = get_users(2000, logged_in=False)
        for user in users:
            user["id"] = None
        self.assert_no_user_experiment(users=users, config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=True), config=config)

    def test_loggedout_experiment_explicit_enable(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "targeting": {"logged_in": [False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_user_experiment_simulation(users=get_users(2000, logged_in=False), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=True), config=config)

    def test_loggedout_experiment_explicit_disable(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": False,
            "experiment": {
                "targeting": {"logged_in": [False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.assert_no_user_experiment(users=get_users(2000, logged_in=False), config=config)
        self.assert_no_user_experiment(users=get_users(2000, logged_in=True), config=config)

    def test_mixed_experiment(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True, False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_user_experiment_simulation(
            users=get_users(1000) + get_users(1000, logged_in=False), config=config
        )

    def test_mixed_experiment_disable(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": False,
            "experiment": {
                "targeting": {"logged_in": [True, False]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.assert_no_user_experiment(
            users=get_users(1000) + get_users(1000, logged_in=False), config=config
        )

    def test_not_loggedin_or_loggedout(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.assert_no_user_experiment(
            users=get_users(1000) + get_users(1000, logged_in=False), config=config
        )

    def test_subreddit_experiment(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "bucket_val": "content_id",
                "targeting": {"content_type": ["subreddit"]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_page_experiment_simulation(
            user=get_users(1)[0], pages=generate_content(2000, "subreddit"), config=config
        )
        self.assert_no_page_experiment(
            user=get_users(1)[0], pages=generate_content(2000, "link"), config=config
        )
        self.assert_no_page_experiment(
            user=get_users(1)[0], pages=generate_content(2000, "comment"), config=config
        )
        self.assert_no_user_experiment(
            users=get_users(1000) + get_users(1000, logged_in=False), config=config
        )

    def test_link_experiment(self):
        config = {
            "id": 1,
            "name": "test",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "bucket_val": "content_id",
                "targeting": {"content_type": ["link", "comment"]},
                "variants": {"larger": 5, "smaller": 10, "control_1": 10, "control_2": 10},
            },
        }
        self.do_page_experiment_simulation(
            user=get_users(1)[0], pages=generate_content(2000, "link"), config=config
        )
        self.do_page_experiment_simulation(
            user=get_users(1)[0], pages=generate_content(2000, "comment"), config=config
        )
        self.assert_no_page_experiment(
            user=get_users(1)[0], pages=generate_content(2000, "subreddit"), config=config
        )
        self.assert_no_user_experiment(
            users=get_users(1000) + get_users(1000, logged_in=False), config=config
        )
