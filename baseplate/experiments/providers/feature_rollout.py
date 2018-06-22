import logging

logger = logging.getLogger(__name__)

from .simple_experiment import SimpleExperiment


class FeatureRollout(SimpleExperiment):
    """Has a single variant (with no control), and buckets the given
    percentage of eligible users into that feature. This "experiment"
    type is meant to be used to roll out a tested, ready feature to users
    at a slower pace. This is _not_ an 'experiment' in the conventional
    sense, as it has no control groups.

    Should not log bucketing events.

    The config dict is expected to have the following values:

        * **variants**: dict mapping variant name to its size. Variant
          sizes are expressed as a fraction of 1 (that is, .15 means 15%,
          not 0.15%).
        * **targeting**: (Optional) dict containing a targeting tree. This
          determines the conditions required for a user to _potentially_
          get bucketed. See <location of targeting classes> for an example
        * **overrides**: (Optional) dict that maps variant names to a
          a targeting dict for users to force into the specified variant.
          If a user falls into multiple overrides, the first variant name
          matched (in lexigraphical order) will be returned.
          When determining the variant of an experiment, the
          override parameters you want to use are passed in as keyword
          arguments to the call to experiment.variant.
        * **bucket_val**: (Optional) Name of the parameter you want to use for
          bucketing.  This value must be passed to the call to
          experiment.variant as a keyword argument.  Defaults to "user_id".
        * **seed**: (Optional) Overrides the seed for this experiment.  If this
          is not set, `id.name` is used as the seed.

    """

    @classmethod
    def from_dict(cls, id, name, owner, start_ts, stop_ts, config,
                  enabled=True, **kwargs):

        return cls(
            id=id,
            name=name,
            owner=owner,
            start_ts=start_ts,
            stop_ts=stop_ts,
            enabled=enabled,
            config=config,
        )

    def should_log_bucketing(self):
        return False

    def _validate_variants(self, variants):

        super(FeatureRollout, self)._validate_variants(variants)

        if len(self.variants) != 1:
            raise ValueError('Rollout can only have a single variant.')

    def _choose_variant(self, bucket):
        """Deterministically choose a percentage-based variant. Every call
        with the same bucket and varaints will result in the same answer

        :param bucket -- an integer bucket representation
        :param variants -- a dictionary of
                           <string:variant name>:<float:percentage> pairs.
        :return string -- the variant name, or None if bucket doesn't fall into
                          any of the variants
        """

        if bucket < self.variants[0]['size'] * self.num_buckets:
            return self.variants[0]['name']

        return None
