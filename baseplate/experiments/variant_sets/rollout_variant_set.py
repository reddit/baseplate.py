from .base import VariantSet


class RolloutVariantSet(VariantSet):
    """VariantSet designed for feature rollouts. Takes a single variant.

    Changing the size of the variant will minimize the treatment of bucketed
    users. Those users going from no treatment to the provided treatment
    (or vice versa) are limited to the change in the provided treatment size.
    For instance, going from 45% to 55% will result in only the new 10% of
    users changing treatments. The initial 45% will not change. Conversely,
    going from 55% to 45% will result in only 10% of users losing the
    treatment.
    """

    def __init__(self, variants, num_buckets=1000):
        """ :param list variants: array of dicts, each containing the keys 'name'
            and 'size'. Name is the variant name, and size is the fraction of
            users to bucket into the corresponding variant. Sizes are expressed
            as a floating point value between 0 and 1.
        :param int num_buckets: the number of potential buckets that can be
            passed in for a variant call. Defaults to 1000, which means maximum
            granularity of 0.1% for bucketing
        """
        # validate before assigning anything on this type, since we're expecting
        # only a single variant
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
        with the same bucket and variants will result in the same answer.

        :param string bucket: an integer bucket representation
        :return string: the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        if bucket < int(self.variant.get('size') * self.num_buckets):
            return self.variant.get('name')

        return None
