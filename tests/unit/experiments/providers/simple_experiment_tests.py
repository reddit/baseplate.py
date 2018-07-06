from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import math
import os
import time
import unittest
import copy

from datetime import datetime, timedelta

from baseplate._compat import iteritems, long, range
from baseplate.core import ServerSpan
from baseplate.events import EventLogger
from baseplate.experiments import ExperimentsContextFactory
from baseplate.experiments.providers import ISO_DATE_FMT, parse_experiment
from baseplate.experiments.providers.simple_experiment import SimpleExperiment
from baseplate.file_watcher import FileWatcher

from .... import mock


THIRTY_DAYS = timedelta(days=30).total_seconds()
FIVE_DAYS = timedelta(days=5).total_seconds()


def get_users(num_users, logged_in=True):
    users = []
    for i in range(num_users):
        if logged_in:
            name = str(i)
        else:
            name = None
        users.append(dict(
            name=name,
            id="t2_%s" % str(i),
            logged_in=logged_in,
        ))
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
            "variants": [
                {
                    "name":"variant_1",
                    "size":0.1,
                },
                {
                    "name":"variant_2",
                    "size":0.1,
                },
            ],
            "experiment_version":1,
        }
    }
    return cfg


def create_simple_experiment():
    cfg = get_simple_config()

    experiment = parse_experiment(cfg)

    return experiment


class TestSimpleExperiment(unittest.TestCase):

    def test_calculate_bucket_value(self):
        experiment = create_simple_experiment()
        experiment.num_buckets = 1000
        self.assertEqual(experiment._calculate_bucket("t2_1"), long(867))

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
                    {
                        "name":"variant_1",
                        "size":0.1,
                    },
                    {
                        "name":"variant_2",
                        "size":0.1,
                    },
                ],
                "experiment_version":1,
                "shuffle_version":1,
                "bucket_seed": "some new seed",
            }
        }

        seeded_experiment = parse_experiment(seeded_cfg)

        self.assertNotEqual(seeded_experiment.seed, experiment.seed)
        self.assertIsNot(seeded_experiment.seed, None)
        seeded_experiment.num_buckets = 1000
        self.assertEqual(
            seeded_experiment._calculate_bucket("t2_1"),
            long(924),
        )

    @unittest.skipIf(os.environ.get("CI") != "true",
                     "test takes too long to run for normal local iteration")
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
            self.assertAlmostEqual(percent_equal, 1.0, delta=.10,
                                   msg='bucket: %s' % bucket)

    @unittest.skipIf(os.environ.get("CI") != "true",
                     "test takes too long to run for normal local iteration")
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
                    {
                        "name":"variant_1",
                        "size":0.1,
                    },
                    {
                        "name":"variant_2",
                        "size":0.1,
                    },
                ],
                "experiment_version":1,
                "shuffle_version":1,
                "bucket_seed": "some_new_seed",
            }
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
            self.assertAlmostEqual(percent_equal, 1.0, delta=.10,
                                   msg='bucket: %s' % bucket)

    @mock.patch('baseplate.experiments.providers.simple_experiment.SimpleExperiment._choose_variant')
    def test_variant_returns_none_if_out_of_time_window(self, choose_variant_mock):
        choose_variant_mock.return_value = 'fake_variant'
        valid_cfg = get_simple_config()
        experiment_valid = parse_experiment(valid_cfg)

        expired_cfg = get_simple_config()
        expired_cfg['stop_ts'] = time.time() - FIVE_DAYS
        experiment_expired = parse_experiment(expired_cfg)

        experiment_not_started_cfg = get_simple_config()
        experiment_not_started_cfg['start_ts'] = time.time() + FIVE_DAYS
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
                    {
                        "name":"variant_1",
                        "size":0.5,
                    },
                    {
                        "name":"variant_2",
                        "size":0.5,
                    },
                ],
                "experiment_version":1,
            }
        }

        experiment = parse_experiment(experiments_cfg)

        variant = experiment.variant(user_id="t2_1")
        self.assertIs(variant, None)

    @mock.patch('baseplate.experiments.providers.simple_experiment.SimpleExperiment._choose_variant')
    def test_bucket_val(self, choose_variant_mock):
        choose_variant_mock.return_value = 'fake_variant'
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
                    {
                        "name":"variant_1",
                        "size":0.5,
                    },
                    {
                        "name":"variant_2",
                        "size":0.5,
                    },
                ],
                "experiment_version":1,
                "bucket_val":"new_bucket_val",
            }
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
        shuffle_cfg['experiment']['shuffle_version'] = 2

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