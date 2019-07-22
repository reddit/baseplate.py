import logging
import unittest

from baseplate.lib.experiments.variant_sets.range_variant_set import RangeVariantSet

logger = logging.getLogger(__name__)


NUM_BUCKETS_DEFAULT = 1000
NUM_BUCKETS_ODD = 1037


def generate_variant_config():
    cfg = [
        {"name": "variant_1", "range_start": 0.0, "range_end": 0.25},
        {"name": "variant_2", "range_start": 0.25, "range_end": 0.5},
        {"name": "variant_3", "range_start": 0.5, "range_end": 0.75},
    ]

    return cfg


def create_range_variant_set():
    cfg = generate_variant_config()
    return RangeVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)


class TestRangeVariantSet(unittest.TestCase):
    def test_validation_passes(self):
        variant_set = create_range_variant_set()
        self.assertTrue(isinstance(variant_set, RangeVariantSet))

        # this is valid by the defined rules of the variant set, despite overlap
        # between ranges. There is a test later down to ensure consistency
        # despite the overlap
        variant_set_cfg_overlap = [
            {"name": "variant_1", "range_start": 0.0, "range_end": 0.25},
            {"name": "variant_2", "range_start": 0.1, "range_end": 0.35},
            {"name": "variant_3", "range_start": 0.35, "range_end": 0.6},
        ]

        RangeVariantSet(variant_set_cfg_overlap)

    def test_validation_fails(self):
        variant_set_cfg_none = None

        variant_set_cfg_0 = []

        variant_set_cfg_too_big = [
            {"name": "variant_1", "range_start": 0.0, "range_end": 0.75},
            {"name": "variant_2", "range_start": 0.0, "range_end": 0.75},
            {"name": "variant_3", "range_start": 0.5, "range_end": 0.75},
        ]

        with self.assertRaises(ValueError):
            RangeVariantSet(variant_set_cfg_none)

        with self.assertRaises(ValueError):
            RangeVariantSet(variant_set_cfg_0)

        with self.assertRaises(ValueError):
            RangeVariantSet(variant_set_cfg_too_big)

    def test_distribution_def_buckets(self):
        variant_set = create_range_variant_set()

        variant_counts = {"variant_1": 0, "variant_2": 0, "variant_3": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 4)

        for variant_count in variant_counts.values():
            self.assertEqual(variant_count, 250)

    def test_distribution_single_bucket(self):
        cfg = [
            {"name": "variant_1", "range_start": 0.0, "range_end": 0.001},
            {"name": "variant_2", "range_start": 0.001, "range_end": 0.001},
            {"name": "variant_3", "range_start": 0.001, "range_end": 0.001},
        ]

        variant_set = RangeVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)

        variant_counts = {"variant_1": 0, "variant_2": 0, "variant_3": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 4)

        self.assertEqual(variant_counts["variant_1"], 1)
        self.assertEqual(variant_counts["variant_2"], 0)
        self.assertEqual(variant_counts["variant_3"], 0)
        self.assertEqual(variant_counts[None], 999)

    def test_distribution_def_odd(self):
        variant_cfg = generate_variant_config()
        variant_cfg.append({"name": "variant_4", "range_start": 0.75, "range_end": 1.0})
        variant_set = RangeVariantSet(variants=variant_cfg, num_buckets=NUM_BUCKETS_ODD)

        variant_counts = {"variant_1": 0, "variant_2": 0, "variant_3": 0, "variant_4": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_ODD):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 5)
        self.assertEqual(variant_counts["variant_1"], 259)
        self.assertEqual(variant_counts["variant_2"], 259)
        self.assertEqual(variant_counts["variant_3"], 259)
        self.assertEqual(variant_counts["variant_4"], 260)

    def test_distribution_multi_range_variant(self):
        cfg = [
            {"name": "variant_1", "range_start": 0.0, "range_end": 0.25},
            {"name": "variant_2", "range_start": 0.25, "range_end": 0.5},
            {"name": "variant_1", "range_start": 0.5, "range_end": 0.75},
        ]

        variant_set = RangeVariantSet(variants=cfg, num_buckets=NUM_BUCKETS_DEFAULT)

        variant_counts = {"variant_1": 0, "variant_2": 0, None: 0}

        for bucket in range(0, NUM_BUCKETS_DEFAULT):
            variant = variant_set.choose_variant(bucket)
            variant_counts[variant] += 1

        self.assertEqual(len(variant_counts), 3)

        self.assertEqual(variant_counts["variant_1"], 500)
        self.assertEqual(variant_counts["variant_2"], 250)
        self.assertEqual(variant_counts[None], 250)

    def test_contains(self):
        variant_set = create_range_variant_set()

        self.assertTrue("variant_2" in variant_set)
        self.assertFalse("variant_7" in variant_set)
