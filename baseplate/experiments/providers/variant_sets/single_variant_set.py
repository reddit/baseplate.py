from .base import VariantSet


class SingleVariantSet(VariantSet):

    def __init__(self, variants, num_buckets=1000):
        self._validate_variants(variants)
        self.variants = variants
        self.num_buckets = num_buckets

    def __contains__(self, item):
        if (self.variants[0].get('name') == item
                or self.variants[1].get('name') == item):
            return True

        return False

    def _validate_variants(self, variants):

        if variants is None:
            raise ValueError('No variants provided')

        if len(variants) != 2:
            raise ValueError("Single Variant experiments expect only one "
                "variant and one control.")

        if variants[0].get('size') is None or variants[1].get('size') is None:
            raise ValueError('Variant size not provided: {}'.format(variants))

        total_size = variants[0].get('size') + variants[1].get('size')

        if total_size < 0.0 or total_size > 1.0:
            raise ValueError('Sum of all variants must be between 0 and 1.')

    def choose_variant(self, bucket):
        """Deterministically choose a percentage-based variant. Every call
        with the same bucket and varaints will result in the same answer

        :param bucket -- an integer bucket representation
        :return string -- the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        if bucket < int(self.variants[0]["size"] * self.num_buckets):
            return self.variants[0]["name"]
        elif bucket >= (self.num_buckets
                - int(self.variants[1]["size"] * self.num_buckets)):
            return self.variants[1]["name"]

        return None
