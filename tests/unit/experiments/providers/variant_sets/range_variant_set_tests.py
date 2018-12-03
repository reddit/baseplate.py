from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import collections
import time
import unittest

from datetime import timedelta

from baseplate._compat import range
from baseplate.experiments.variant_sets.range_variant_set import RangeVariantSet

logger = logging.getLogger(__name__)


NUM_BUCKETS_DEFAULT = 1000
NUM_BUCKETS_ODD = 1037


def generate_variant_config():
    cfg = [
        {"name": "variant_1", "range": [0, 0.25]},
        {"name": "variant_2", "range": [0.25, 0.5]},
        {"name": "variant_3", "range": [0.5,0.75]},
    ]

    return cfg

def create_range_variant_set():
    cfg = generate_variant_config()
    return RangeVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)


class TestRangeVariantSet(unittest.TestCase):

    def test_validation_passes(self):
        variant_set = create_range_variant_set()

        self.assertTrue(isinstance(variant_set, RangeVariantSet))

    def test_validation_fails(self):
        variant_set_cfg_none = None

        variant_set_cfg_0 = [
        ]

        variant_set_cfg_too_big = [
            {"name": "variant_1", "range": [0, 0.75]},
            {"name": "variant_2", "range": [0, 0.75]},
            {"name": "variant_3", "range": [0.25, 0.5]},
        ]

        variant_set_cfg_overlap = [
            {"name": "variant_1", "range": [0, 0.25]},
            {"name": "variant_2", "range": [0.2, 0.45]},
            {"name": "variant_3", "range": [0.4,0.65]},
        ]

        with self.assertRaises(ValueError):
            variant_set_none = RangeVariantSet(variant_set_cfg_none)
        
        with self.assertRaises(ValueError):
            variant_set_too_big = RangeVariantSet(variant_set_cfg_too_big)

        # should not raise unless overlap validation is added
        variant_set_overlap = RangeVariantSet(variant_set_cfg_overlap)

    def test_distribution_def_buckets(self):
        variant_set = create_range_variant_set()

        variant_counts = {
            "variant_1": 0,
            "variant_2": 0,
            "variant_3": 0,
            None: 0,
        }

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 4)
        logger.error(variant_counts)

        for variant_count in variant_counts.values():
            self.assertEqual(variant_count, 250)

    def test_distribution_single_bucket(self):
        cfg = [
            {"name": "variant_1", "range": [0, 0.001]},
            {"name": "variant_2", "range": [0, 0]},
            {"name": "variant_3", "range": [0, 0]},
        ]

        variant_set = RangeVariantSet(
            variants=cfg, 
            num_buckets=NUM_BUCKETS_DEFAULT
        )

        variant_counts = {
            "variant_1": 0,
            "variant_2": 0,
            "variant_3": 0,
            None: 0,
        }

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 4)

        self.assertEqual(variant_counts['variant_1'], 1)
        self.assertEqual(variant_counts['variant_2'], 0)
        self.assertEqual(variant_counts['variant_3'], 0)
        self.assertEqual(variant_counts[None], 999)

    def test_distribution_def_odd(self):
        variant_cfg = generate_variant_config()
        variant_cfg.append({"name": "variant_4", "range": [0.75, 1]})
        variant_set = RangeVariantSet(
            variants=variant_cfg, 
            num_buckets=NUM_BUCKETS_ODD,
        )

        variant_counts = {
            "variant_1": 0,
            "variant_2": 0,
            "variant_3": 0,
            "variant_4": 0,
            None: 0,
        }

        for bucket in range(0,NUM_BUCKETS_ODD):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 5)
        self.assertEqual(variant_counts["variant_1"], 259)
        self.assertEqual(variant_counts["variant_2"], 259)
        self.assertEqual(variant_counts["variant_3"], 259)
        self.assertEqual(variant_counts["variant_4"], 260)

    def test_contains(self):
        variant_set = create_range_variant_set()

        self.assertTrue("variant_2" in variant_set)
        self.assertFalse("variant_7" in variant_set)

    def test_distribution_gapped_buckets(self):
        cfg_with_gaps = [
            {"name": "variant_1", "range": [0, 0.25]},
            {"name": "variant_2", "range": [0.3, 0.55]},
            {"name": "variant_3", "range": [0.65,0.90]},
        ]

        variant_set = RangeVariantSet(variants=cfg_with_gaps)

        variant_counts = {
            "variant_1": 0,
            "variant_2": 0,
            "variant_3": 0,
            None: 0,
        }

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 4)

        for variant_count in variant_counts.values():
            self.assertEqual(variant_count, 250)


