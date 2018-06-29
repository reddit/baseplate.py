from .base import VariantSet


class RolloutVariantSet(VariantSet):

    def __init__(self, variants, num_buckets=1000):
        self._validate_variants(variants)
        self.variant = variants[0]
        self.num_buckets = num_buckets

    def __contains__(self, item):
        return self.variant.get('name') == item

    def _validate_variants(self, variants):

        if variants is None:
            raise ValueError('No variants provided')

        if len(variants) != 1:
            raise ValueError("Rollout variant only supports one variant.")

        size = variants[0].get('size')
        if size is None or size < 0.0 or size > 1.0:
            raise ValueError('Variant size must be between 0 and 1')

    def choose_variant(self, bucket):
        """Deterministically choose a percentage-based variant. Every call
        with the same bucket and varaints will result in the same answer.

        :param bucket -- an integer bucket representation
        :return string -- the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        if bucket < int(self.variant.get('size') * self.num_buckets):
            return self.variant.get('name')

        return None
