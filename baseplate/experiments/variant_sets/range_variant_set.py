from .base import VariantSet


class RangeVariantSet(VariantSet):
    """ Variant Set designed to take fixed bucket ranges.

    This VariantSet allows manually setting bucketing ranges.
    It takes in a variant name, then the range of buckets in
    that should be assigned to that variant. This enables user-defined
    bucketing algorithms, as well as simplifies the ability to adjust
    range sizes in special circumstances.
    """

    def __init__(self, variants, num_buckets=1000):
        """ :param list variants: array of dicts, each containing the keys 'name',
            'range'. Name is the variant name, and range is a list containing the lower
            (inclusive) and upper (exclusive) ends of the bucketing range.
            Lower and upper values are expressed as floating point values between 0 and 1,
            and will be truncated to match the granularity of the number of buckets. This
            function does not validate that ranges do not overlap.
        :param int num_buckets: the number of potential buckets that can be
            passed in for a variant call. Defaults to 1000, which means maximum
            granularity of 0.1% for bucketing
        """

        self.variants = variants
        self.num_buckets = num_buckets

        self._validate_variants()

    def __contains__(self, item):
        for variant in self.variants:
            if variant.get('name') == item:
                return True

        return False

    def _validate_variants(self):

        if not self.variants or len(self.variants) == 0:
            raise ValueError('No variants provided')

        total_size = 0
        for variant in self.variants:
            variant_range = variant.get('range')
            if not variant_range or len(variant_range) != 2:
                raise ValueError('Variant range not provided: {}'.format(self.variants))

            variant['bucket_lower'] = int(variant_range[0] * self.num_buckets)
            variant['bucket_upper'] = int(variant_range[1] * self.num_buckets)

            total_size += variant['bucket_upper'] - variant['bucket_lower']

        if total_size > self.num_buckets:
            raise ValueError('Sum of all variants is greater than 100%')

    def choose_variant(self, bucket):
        """Deterministically choose a variant. Every call with the same bucket
        on one instance will result in the same answer

        :param int bucket: an integer bucket representation
        :return string: the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        for variant in self.variants:
            if bucket >= variant['bucket_lower'] and bucket < variant['bucket_upper']:
                return variant.get('name')

        return None
