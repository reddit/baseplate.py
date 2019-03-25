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
        """ :param list variants: Array of dicts, each containing the keys 'name',
            'range_start', and 'range_end'. Name is the variant name, while 'range_start'
            and 'range_end' are the start (inclusive) and end (exclusive) of the bucketing
            range.  The latter two are expressed as a floating point value between 0 and 1,
            which will be applied to a the relevant bucket (multiplied by the num_buckets,
            and cast to an integer).
        :param int num_buckets: The number of potential buckets that can be
            passed in for a variant call. Defaults to 1000, which means maximum
            granularity of 0.1% for bucketing
        """
        self.num_buckets = num_buckets
        self.variants = variants

        self._validate_variants()

    def __contains__(self, item):
        for variant in self.variants:
            if variant.get('name') == item:
                return True

        return False

    def _validate_variants(self):

        if self.variants is None:
            raise ValueError('No variants provided')

        if len(self.variants) < 1:
            raise ValueError("RangeVariant experiments expect at least one variant")

        total_size = 0
        for variant in self.variants:
            if variant.get('range_start') is None or variant.get('range_end') is None:
                raise ValueError('Variant range invalid: {}'.format(self.variants))
            range_size = variant.get('range_end') - variant.get('range_start')
            total_size += int(range_size * self.num_buckets)

        if total_size > self.num_buckets:
            raise ValueError('Sum of all variants is greater than 100%')

    def choose_variant(self, bucket):
        """Deterministically choose a variant. Every call with the same bucket
        on one instance will result in the same answer

        :param string bucket: an integer bucket representation
        :return string: the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        for variant in self.variants:
            if (bucket >= int(variant['range_start'] * self.num_buckets) and
                    bucket < int(variant['range_end'] * self.num_buckets)):
                return variant['name']

        return None
