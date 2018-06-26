from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import math
import os
import time
import unittest

from datetime import datetime, timedelta

from baseplate._compat import iteritems, long, range
from baseplate.core import ServerSpan
from baseplate.events import EventLogger
from baseplate.experiments import ExperimentsContextFactory
from baseplate.experiments.providers import ISO_DATE_FMT, parse_experiment
from baseplate.experiments.providers.multi_variant import MultiVariantExperiment
from baseplate.file_watcher import FileWatcher

from .... import mock


THIRTY_DAYS = timedelta(days=30).total_seconds()


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


def generate_test_config():
    cfg = {
            "id": 1,
            "name": "test_experiment",
            "owner": "test",
            "type": "multi_variant",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "enabled": True,
            "experiment": {
                "variants": [
                    {
                        "name":"control_1",
                        "size":0.1,
                    },
                    {
                        "name":"control_2",
                        "size":0.1,
                    },
                    {
                        "name":"variant_1",
                        "size":0.1,
                    },
                ],
                "experiment_version":"1",
                "version": "1"
            },
        }

    return cfg


class TestMultiVariantExperiment(unittest.TestCase):

    def test_multi_variant_type_returns_mv_experiment(self):
        cfg = generate_test_config()
        experiment = parse_experiment(cfg)
        self.assertTrue(isinstance(experiment, MultiVariantExperiment))
        self.assertTrue(experiment.should_log_bucketing())
        self.assertEqual(experiment.version, '1')

    def test_calculate_bucket_value(self):
        cfg = generate_test_config()
        experiment = parse_experiment(cfg)
        experiment.num_buckets = 1000
        self.assertEqual(experiment._calculate_bucket("t2_1"), long(867))

        cfg = generate_test_config()
        cfg['experiment']['bucket_seed'] = 'test_seed'
        
        seeded_experiment = parse_experiment(cfg)
        self.assertNotEqual(seeded_experiment.seed, experiment.seed)
        self.assertIsNot(seeded_experiment.seed, None)
        seeded_experiment.num_buckets = 1000
        self.assertEqual(
            seeded_experiment._calculate_bucket("t2_1"),
            long(353),
        )

    def test_multi_variant_validation(self):
        cfg = generate_test_config()
        two_variant = [{"name":"variant_1","size":0.1},
                       {"name":"variant_2","size":0.1}]
        cfg['experiment']['variants'] = two_variant
        with self.assertRaises(ValueError):
            parse_experiment(cfg)

        cfg = generate_test_config()
        additional_variant = {"name":"variant_3","size":0.1}
        cfg['experiment']['variants'].append(additional_variant)
        parse_experiment(cfg)

        cfg = generate_test_config()
        variants = [{"name":"variant_1","size":0.75},
                    {"name":"variant_2","size":0.50},
                    {"name":"control_1","size":0.1}]
        cfg['experiment']['variants'] = variants
        with self.assertRaises(ValueError):
            parse_experiment(cfg)

    def test_variant_distribution(self):
        cfg = generate_test_config()
        experiment = parse_experiment(cfg)
        variant_counts = {"control_1":0,
                          "control_2":0,
                          "variant_1":0,
                          None:0}

        for bucket in range(0,experiment.num_buckets):
            variant = experiment._choose_variant(bucket)
            variant_counts[variant] = variant_counts[variant] + 1

        self.assertEqual(variant_counts["variant_1"], 100)
        self.assertEqual(variant_counts["control_1"], 100)
        self.assertEqual(variant_counts["control_2"], 100)
        self.assertEqual(variant_counts[None], 700)

    @unittest.skipIf(os.environ.get("CI") != "true",
                     "test takes too long to run for normal local iteration")
    def test_calculate_bucket(self):
        cfg = generate_test_config()
        experiment = parse_experiment(cfg)

        # Give ourselves enough users that we can get some reasonable amount of
        # precision when checking amounts per bucket. This fails with 1000.
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
                                   msg='bucket: {}'.format(bucket))

    @unittest.skipIf(os.environ.get("CI") != "true",
                     "test takes too long to run for normal local iteration")
    def test_calculate_bucket_with_seed(self):
        cfg = generate_test_config()
        cfg['experiment']['bucket_seed'] = 'test_seed'
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
            self.assertEqual(experiment.seed, "test_seed")
            bucket1 = experiment._calculate_bucket(fullname)
            counter[bucket1] += 1
            # Ensure bucketing is deterministic.
            self.assertEqual(bucket1, experiment._calculate_bucket(fullname))

            current_seed = experiment.seed
            experiment._seed = "new_test_seed"
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