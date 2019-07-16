import logging
import unittest

from baseplate.lib.experiments.variant_sets.rollout_variant_set import RolloutVariantSet

logger = logging.getLogger(__name__)


NUM_BUCKETS_DEFAULT = 1000
NUM_BUCKETS_ODD = 1037


def generate_variant_config():
    cfg = [{"name": "variant_1", "size": 0.25}]

    return cfg


def create_rollout_variant_set():
    cfg = generate_variant_config()
    return RolloutVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)


class TestRolloutVariantSet(unittest.TestCase):
    def test_validation_passes(self):
        variant_set = create_rollout_variant_set()

        self.assertTrue(isinstance(variant_set, RolloutVariantSet))

    def test_validation_fails(self):
        variant_set_cfg_none = None

        variant_set_cfg_0 = []

        variant_set_cfg_2 = [
            {"name": "variant_1", "size": 0.25},
            {"name": "variant_2", "size": 0.25},
        ]

        variant_set_cfg_too_big = [{"name": "variant_1", "size": 1.05}]

        with self.assertRaises(ValueError):
            RolloutVariantSet(variant_set_cfg_none)

        with self.assertRaises(ValueError):
            RolloutVariantSet(variant_set_cfg_0)

        with self.assertRaises(ValueError):
            RolloutVariantSet(variant_set_cfg_2)

        with self.assertRaises(ValueError):
            RolloutVariantSet(variant_set_cfg_too_big)

    def test_distribution_def_buckets(self):
        variant_set = create_rollout_variant_set()

        variant_counts = {"variant_1": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 2)

        self.assertEqual(variant_counts["variant_1"], 250)
        self.assertEqual(variant_counts[None], 750)

    def test_distribution_single_bucket(self):
        cfg = [{"name": "variant_1", "size": 0.001}]

        variant_set = RolloutVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)

        variant_counts = {"variant_1": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 2)

        self.assertEqual(variant_counts["variant_1"], 1)
        self.assertEqual(variant_counts[None], 999)

    def test_distribution_def_odd(self):
        variant_cfg = [{"name": "variant_1", "size": 1.0}]
        variant_set = RolloutVariantSet(variants=variant_cfg, num_buckets=NUM_BUCKETS_ODD)

        variant_counts = {"variant_1": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_ODD):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 2)
        self.assertEqual(variant_counts["variant_1"], 1037)
        self.assertEqual(variant_counts[None], 0)

    def test_contains(self):
        variant_set = create_rollout_variant_set()

        self.assertTrue("variant_1" in variant_set)
        self.assertFalse("variant_7" in variant_set)
