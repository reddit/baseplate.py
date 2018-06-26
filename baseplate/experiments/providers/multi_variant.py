import logging

logger = logging.getLogger(__name__)

from .simple_experiment import SimpleExperiment


class MultiVariantExperiment(SimpleExperiment):
    """Basic experiment, handling more than two variants (typically two
    controls, and multiple treatments).
    This type of experiment does not guarantee that changing the variant size
    will not rebucket existing users.

    Does log bucketing events.

    The config dict is expected to have the following values:

        * **variants**: array of dicts, each containing the keys 'name'
          and 'size'. Name is the variant name, and size is the fraction of
          users to bucket into the corresponding variant. Sizes are expressed
          as a floating point value between 0 and 1.
        * **bucket_val**: (Optional) Name of the parameter you want to use for
          bucketing.  This value must be passed to the call to
          experiment.variant as a keyword argument.  Defaults to "user_id".
        * **seed**: (Optional) Overrides the seed for this experiment.  If this
          is not set, a combination of the experiment name, id, and shuffle
          version is used.
    """

    def _validate_variants(self, variants):

        super(MultiVariantExperiment, self)._validate_variants(variants)

        if len(variants) < 3:
            raise ValueError("MultiVariant experiments expect two controls "
                "and at least one variant.")

    def _choose_variant(self, bucket):
        """Deterministically choose a percentage-based variant. Every call
        with the same bucket and varaints will result in the same answer.

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
