from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from baseplate.lib.experiments.variant_sets.base import VariantSet


class MultiVariantSet(VariantSet):
    """Variant Set designed to handle more than two total treatments.

    MultiVariantSets are not designed to support changes in variant sizes without
    rebucketing.

    :param variants: Array of dicts, each containing the keys 'name'
        and 'size'. Name is the variant name, and size is the fraction of
        users to bucket into the corresponding variant. Sizes are expressed
        as a floating point value between 0 and 1.
    :param num_buckets: The number of potential buckets that can be
        passed in for a variant call. Defaults to 1000, which means maximum
        granularity of 0.1% for bucketing

    """

    # pylint: disable=super-init-not-called
    def __init__(self, variants: List[Dict[str, Any]], num_buckets: int = 1000):
        self.num_buckets = num_buckets
        self.variants = variants

        self._validate_variants()

    def __contains__(self, item: str) -> bool:
        for variant in self.variants:
            if variant.get("name") == item:
                return True

        return False

    def _validate_variants(self) -> None:
        if self.variants is None:
            raise ValueError("No variants provided")

        if len(self.variants) < 3:
            raise ValueError("MultiVariant experiments expect three or more variants.")

        total_size = 0
        for variant in self.variants:
            if variant.get("size") is None:
                raise ValueError(f"Variant size not provided: {self.variants}")
            total_size += int(variant["size"] * self.num_buckets)

        if total_size > self.num_buckets:
            raise ValueError("Sum of all variants is greater than 100%")

    def choose_variant(self, bucket: int) -> Optional[str]:
        """Deterministically choose a variant.

        Every call with the same bucket on one instance will result in the same
        answer

        :param bucket: an integer bucket representation
        :return: the variant name, or None if bucket doesn't fall into any of
            the variants
        """
        current_offset = 0

        for variant in self.variants:
            current_offset += int(variant["size"] * self.num_buckets)
            if bucket < current_offset:
                return variant["name"]

        return None
