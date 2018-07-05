from .base import VariantSet


class MultiVariantSet(VariantSet):
    """ Variant Set designed to handle more than two total treatments.

    MultiVariantSets are not designed to support changes in variant sizes without
    rebucketing.
    """

    def __init__(self, variants, num_buckets=1000):
        """ :param variants -- array of dicts, each containing the keys 'name'
            and 'size'. Name is the variant name, and size is the fraction of
            users to bucket into the corresponding variant. Sizes are expressed
            as a floating point value between 0 and 1.
        :param num_buckets -- the number of potential buckets that can be
            passed in for a variant call. Defaults to 1000, which means maximum
            granularity of 0.1% for bucketing
        """
        self._validate_variants(variants)
        self.variants = variants
        self.num_buckets = num_buckets

    def __contains__(self, item):
        for variant in self.variants:
            if variant.get('name') == item:
                return True

        return False

    def _validate_variants(self, variants):

        if variants is None:
            raise ValueError('No variants provided')

        if len(variants) < 3:
            raise ValueError("MultiVariant experiments expect two controls "
                "and at least one variant.")

        total_size = 0.0
        for variant in variants:
            if variant.get('size') is None:
                raise ValueError('Variant size not provided: {}'.format(variants))
            total_size += variant.get('size')

        if total_size > 1.0:
            raise ValueError('Sum of all variants is greater than 100%')

    def choose_variant(self, bucket):
        """Deterministically choose a variant. Every call with the same bucket
        on one instance will result in the same answer

        :param bucket -- an integer bucket representation
        :return string -- the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        current_offset = 0

        for variant in self.variants:
            current_offset += int(variant['size'] * self.num_buckets)
            if bucket < current_offset:
                return variant['name']

        return None
