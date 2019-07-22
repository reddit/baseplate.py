import collections
import os
import time
import unittest

from datetime import timedelta
from unittest import mock

from baseplate.lib.experiments.providers import parse_experiment
from baseplate.lib.experiments.providers.simple_experiment import _generate_overrides
from baseplate.lib.experiments.providers.simple_experiment import _generate_targeting
from baseplate.lib.experiments.targeting.base import Targeting
from baseplate.lib.experiments.targeting.tree_targeting import EqualNode
from baseplate.lib.experiments.targeting.tree_targeting import OverrideNode


THIRTY_DAYS = timedelta(days=30).total_seconds()
FIVE_DAYS = timedelta(days=5).total_seconds()


def get_users(num_users, logged_in=True):
    users = []
    for i in range(num_users):
        if logged_in:
            name = str(i)
        else:
            name = None
        users.append(dict(name=name, id="t2_%s" % str(i), logged_in=logged_in))
    return users


def choose_variants_override(self, **kwargs):
    return "fake_variant"


def get_simple_config():
    cfg = {
        "id": 1,
        "name": "test_experiment",
        "owner": "test",
        "type": "single_variant",
        "version": "1",
        "start_ts": time.time() - THIRTY_DAYS,
        "stop_ts": time.time() + THIRTY_DAYS,
        "enabled": True,
        "experiment": {
            "variants": [{"name": "variant_1", "size": 0.1}, {"name": "variant_2", "size": 0.1}],
            "experiment_version": 1,
        },
    }
    return cfg


def create_simple_experiment():
    cfg = get_simple_config()

    experiment = parse_experiment(cfg)

    return experiment


def get_targeting_config():
    targeting_cfg = {
        "ALL": [
            {
                "ANY": [
                    {"EQ": {"field": "is_mod", "value": True}},
                    {"EQ": {"field": "user_id", "values": ["t2_1", "t2_2", "t2_3", "t2_4"]}},
                ]
            },
            {"NOT": {"EQ": {"field": "is_pita", "value": True}}},
            {"EQ": {"field": "is_logged_in", "values": [True, False]}},
            {"NOT": {"EQ": {"field": "subreddit_id", "values": ["t5_1", "t5_2"]}}},
            {
                "ALL": [
                    {"EQ": {"field": "random_numeric", "values": [1, 2, 3, 4, 5]}},
                    {"EQ": {"field": "random_numeric", "value": 5}},
                ]
            },
        ]
    }
    return targeting_cfg


def get_simple_override_config():
    override_config = [
        {"override_variant_1": {"EQ": {"field": "user_id", "value": "t2_1"}}},
        {"override_variant_2": {"EQ": {"field": "user_id", "value": "t2_2"}}},
        {"override_variant_3": {"EQ": {"field": "user_id", "values": ["t2_2", "t2_3"]}}},
        {"override_variant_1": {"EQ": {"field": "user_id", "values": ["t2_1", "t2_4"]}}},
    ]

    return override_config


def get_dict_override_config():
    override_config = {
        "override_variant_1": {"EQ": {"field": "user_id", "value": "t2_1"}},
        "override_variant_2": {"EQ": {"field": "user_id", "value": "t2_2"}},
        "override_variant_3": {"EQ": {"field": "user_id", "values": ["t2_2", "t2_3"]}},
    }

    return override_config


class TestSimpleExperiment(unittest.TestCase):
    def test_calculate_bucket_value(self):
        experiment = create_simple_experiment()
        experiment.num_buckets = 1000
        self.assertEqual(experiment._calculate_bucket("t2_1"), int(867))

        seeded_cfg = {
            "id": 1,
            "name": "test_experiment",
            "owner": "test",
            "type": "single_variant",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "variants": [
                    {"name": "variant_1", "size": 0.1},
                    {"name": "variant_2", "size": 0.1},
                ],
                "experiment_version": 1,
                "shuffle_version": 1,
                "bucket_seed": "some new seed",
            },
        }

        seeded_experiment = parse_experiment(seeded_cfg)

        self.assertNotEqual(seeded_experiment.seed, experiment.seed)
        self.assertIsNot(seeded_experiment.seed, None)
        seeded_experiment.num_buckets = 1000
        self.assertEqual(seeded_experiment._calculate_bucket("t2_1"), int(924))

    @unittest.skipIf(
        "CI" not in os.environ, "test takes too long to run for normal local iteration"
    )
    def test_calculate_bucket(self):
        experiment = create_simple_experiment()

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket.
        num_users = experiment.num_buckets * 2000
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
        seeded_cfg = {
            "id": 1,
            "name": "test_experiment",
            "owner": "test",
            "type": "single_variant",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "variants": [
                    {"name": "variant_1", "size": 0.1},
                    {"name": "variant_2", "size": 0.1},
                ],
                "experiment_version": 1,
                "shuffle_version": 1,
                "bucket_seed": "some_new_seed",
            },
        }

        experiment = parse_experiment(seeded_cfg)

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket.
        num_users = experiment.num_buckets * 2000
        fullnames = []
        for i in range(num_users):
            fullnames.append("t2_%s" % str(i))

        counter = collections.Counter()
        bucketing_changed = False
        for fullname in fullnames:
            self.assertEqual(experiment.seed, "some_new_seed")
            bucket1 = experiment._calculate_bucket(fullname)
            counter[bucket1] += 1
            # Ensure bucketing is deterministic.
            self.assertEqual(bucket1, experiment._calculate_bucket(fullname))

            current_seed = experiment.seed
            experiment._seed = "newstring"
            bucket2 = experiment._calculate_bucket(fullname)
            experiment._seed = current_seed
            # check that the bucketing changed at some point. Can't compare
            # bucket1 to bucket2 inline because sometimes the user will fall
            # into both buckets, and test will fail.
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

    @mock.patch(
        "baseplate.lib.experiments.providers.simple_experiment.SimpleExperiment._choose_variant"
    )
    def test_variant_returns_none_if_out_of_time_window(self, choose_variant_mock):
        choose_variant_mock.return_value = "fake_variant"
        valid_cfg = get_simple_config()
        experiment_valid = parse_experiment(valid_cfg)

        expired_cfg = get_simple_config()
        expired_cfg["stop_ts"] = time.time() - FIVE_DAYS
        experiment_expired = parse_experiment(expired_cfg)

        experiment_not_started_cfg = get_simple_config()
        experiment_not_started_cfg["start_ts"] = time.time() + FIVE_DAYS
        experiment_not_started = parse_experiment(experiment_not_started_cfg)

        variant_valid = experiment_valid.variant(user_id="t2_1")
        variant_expired = experiment_expired.variant(user_id="t2_1")
        variant_not_started = experiment_not_started.variant(user_id="t2_1")

        self.assertIsNot(variant_valid, None)
        self.assertIs(variant_expired, None)
        self.assertIs(variant_not_started, None)

    def test_no_bucket_val(self):
        experiment = create_simple_experiment()
        no_user_id_provided_variant = experiment.variant(not_user_id="t2_1")
        none_user_id_provided_variant = experiment.variant(not_user_id=None)

        self.assertIs(no_user_id_provided_variant, None)
        self.assertIs(none_user_id_provided_variant, None)

    def test_experiment_disabled(self):
        experiments_cfg = {
            "id": 1,
            "name": "test_experiment",
            "owner": "test",
            "type": "single_variant",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": False,
            "experiment": {
                "variants": [
                    {"name": "variant_1", "size": 0.5},
                    {"name": "variant_2", "size": 0.5},
                ],
                "experiment_version": 1,
            },
        }

        experiment = parse_experiment(experiments_cfg)

        variant = experiment.variant(user_id="t2_1")
        self.assertIs(variant, None)

    @mock.patch(
        "baseplate.lib.experiments.providers.simple_experiment.SimpleExperiment._choose_variant"
    )
    def test_bucket_val(self, choose_variant_mock):
        choose_variant_mock.return_value = "fake_variant"
        cfg = {
            "id": 1,
            "name": "test_experiment",
            "owner": "test",
            "type": "single_variant",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "variants": [
                    {"name": "variant_1", "size": 0.5},
                    {"name": "variant_2", "size": 0.5},
                ],
                "experiment_version": 1,
                "bucket_val": "new_bucket_val",
            },
        }

        experiment = parse_experiment(cfg)

        experiment._choose_variant = choose_variants_override

        variant_default_bucket_val = experiment.variant(user_id="t2_1")
        variant_new_bucket_val = experiment.variant(new_bucket_val="some_value")

        self.assertIs(variant_default_bucket_val, None)
        self.assertIsNot(variant_new_bucket_val, None)

    def test_change_shuffle_version_changes_bucketing(self):
        cfg = get_simple_config()
        experiment_version_1 = parse_experiment(cfg)

        shuffle_cfg = get_simple_config()
        shuffle_cfg["experiment"]["shuffle_version"] = 2

        experiment_version_2 = parse_experiment(shuffle_cfg)

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket.
        num_users = experiment_version_1.num_buckets * 100
        fullnames = []
        for i in range(num_users):
            fullnames.append("t2_%s" % str(i))

        counter = collections.Counter()
        bucketing_changed = False
        for fullname in fullnames:
            bucket1 = experiment_version_1._calculate_bucket(fullname)
            counter[bucket1] += 1
            # Ensure bucketing is deterministic.
            self.assertEqual(bucket1, experiment_version_1._calculate_bucket(fullname))

            bucket2 = experiment_version_2._calculate_bucket(fullname)
            # check that the bucketing changed at some point. Can't compare
            # bucket1 to bucket2 inline because sometimes the user will fall
            # into both buckets, and test will fail. When a user doesn't match,
            # break out of loop
            if bucket1 != bucket2:
                bucketing_changed = True
                break

        self.assertTrue(bucketing_changed)

    def test_generate_targeting_valid_config(self):
        targeting_cfg = get_targeting_config()

        experiment_correct_targeting = _generate_targeting(targeting_cfg)
        self.assertTrue(isinstance(experiment_correct_targeting, Targeting))

        self.assertFalse(isinstance(experiment_correct_targeting, OverrideNode))

    def test_generate_targeting_no_config(self):
        targeting_cfg = None

        experiment_correct_targeting = _generate_targeting(targeting_cfg)
        self.assertTrue(isinstance(experiment_correct_targeting, Targeting))

        self.assertTrue(isinstance(experiment_correct_targeting, OverrideNode))
        self.assertTrue(experiment_correct_targeting.evaluate())

    def test_generate_targeting_invalid_config(self):
        targeting_cfg = get_targeting_config()
        targeting_cfg["ALL"][0]["ANY"][0] = {"EQUAL": {"field": "is_mod", "value": True}}

        experiment_correct_targeting = _generate_targeting(targeting_cfg)
        self.assertTrue(isinstance(experiment_correct_targeting, Targeting))

        self.assertTrue(isinstance(experiment_correct_targeting, OverrideNode))
        self.assertFalse(experiment_correct_targeting.evaluate())

    def test_targeting_in_config(self):
        cfg = get_simple_config()
        targeting_cfg = get_targeting_config()
        cfg["experiment"]["targeting"] = targeting_cfg

        experiment_with_targeting = parse_experiment(cfg)

        self.assertTrue(
            experiment_with_targeting.is_targeted(is_mod=True, is_logged_in=True, random_numeric=5)
        )

    def test_construct_override(self):
        cfg = get_simple_override_config()
        overrides = _generate_overrides(cfg)

        self.assertEqual(len(overrides), 4)

        override_names_and_types = [
            ("override_variant_1", EqualNode),
            ("override_variant_2", EqualNode),
            ("override_variant_3", EqualNode),
            ("override_variant_1", EqualNode),
        ]

        for i, override in enumerate(overrides):
            self.assertEqual(len(overrides[i]), 1)
            variant_name, override_type = override_names_and_types[i]
            self.assertTrue(isinstance(override[variant_name], override_type))

    def test_construct_override_dict_input(self):
        cfg = get_dict_override_config()
        overrides = _generate_overrides(cfg)

        self.assertIs(overrides, None)

    def test_construct_invalid_override(self):
        cfg = get_simple_override_config()
        cfg[1]["override_variant_2"] = {"EQUAL": {"field": "user_id", "value": "t2_1"}}
        overrides = _generate_overrides(cfg)

        override_names_and_types = [
            ("override_variant_1", EqualNode),
            ("override_variant_2", OverrideNode),
            ("override_variant_3", EqualNode),
            ("override_variant_1", EqualNode),
        ]

        for i, override in enumerate(overrides):
            self.assertEqual(len(overrides[i]), 1)
            variant_name, override_type = override_names_and_types[i]
            self.assertTrue(isinstance(override[variant_name], override_type))

        self.assertEqual(len(overrides), 4)

    def test_construct_invalid_overrides(self):
        cfg = get_simple_override_config()
        cfg[0] = "not a dictionary"
        overrides = _generate_overrides(cfg)

        override_names_and_types = [
            ("override_variant_2", EqualNode),
            ("override_variant_3", EqualNode),
            ("override_variant_1", EqualNode),
        ]

        for i, override in enumerate(overrides):
            self.assertEqual(len(overrides[i]), 1)
            variant_name, override_type = override_names_and_types[i]
            self.assertTrue(isinstance(override[variant_name], override_type))

        self.assertEqual(len(overrides), 3)

    def test_get_override(self):
        exp_config = get_simple_config()
        override_config = get_simple_override_config()
        exp_config["experiment"]["overrides"] = override_config

        experiment_with_overrides = parse_experiment(exp_config)

        self.assertEqual(
            experiment_with_overrides.get_override(user_id="t2_1"), "override_variant_1"
        )

        self.assertEqual(
            experiment_with_overrides.get_override(user_id="t2_2"), "override_variant_2"
        )

        self.assertEqual(
            experiment_with_overrides.get_override(user_id="t2_3"), "override_variant_3"
        )

        self.assertEqual(
            experiment_with_overrides.get_override(user_id="t2_4"), "override_variant_1"
        )

    @mock.patch(
        "baseplate.lib.experiments.providers.simple_experiment.SimpleExperiment._choose_variant"
    )
    def test_variant_call_with_overrides(self, choose_variant_mock):
        choose_variant_mock.return_value = "mocked_variant"

        exp_config = get_simple_config()
        override_config = get_simple_override_config()
        exp_config["experiment"]["overrides"] = override_config

        experiment_with_overrides = parse_experiment(exp_config)

        self.assertEqual(experiment_with_overrides.variant(user_id="t2_1"), "override_variant_1")

        self.assertEqual(experiment_with_overrides.variant(user_id="t2_2"), "override_variant_2")

        self.assertEqual(experiment_with_overrides.variant(user_id="t2_3"), "override_variant_3")

        self.assertEqual(experiment_with_overrides.variant(user_id="t2_4"), "override_variant_1")

        self.assertEqual(experiment_with_overrides.variant(user_id="t2_5"), "mocked_variant")
