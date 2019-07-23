from typing import Any
from typing import Dict

from baseplate.lib.experiments.providers.r2 import R2Experiment


class FeatureFlag(R2Experiment):
    """An experiment with a single variant "active".

    .. deprecated:: 0.27
       Use SimpleExperiment with RolloutVariantSet instead.

    Does not log bucketing events to the event pipeline.  Use this type of
    experiment if you just want to control access to a feature but do not want
    to run an actual experiment.  Some examples for when you would want to use
    a FeatureFlag are:

    1. Slowly rolling out a new feature to a % of users
    2. Restricting a new feature to certain subreddits

    The config dict is expected to have the following values:

        * **variants**: dict mapping variant names to their sizes. Variant
          sizes are expressed as numeric percentages rather than a fraction of
          1 (that is, 1.5 means 1.5%, not 150%).  For a feature flag, you can
          only specify a single variant named "active".
        * **targeting**: (Optional) dict that maps the names of targeting
          parameters to lists of valid values.  When determining the variant
          of an experiment, the targeting parameters you want to use are passed
          in as keyword arguments to the call to experiment.variant.
        * **overrides**: (Optional) dict that maps override parameters to
          dictionaries mapping values to the variant name you want to override
          the variant to. When determining the variant of an experiment, the
          override parameters you want to use are passed in as keyword
          arguments to the call to experiment.variant.
        * **bucket_val**: (Optional) Name of the parameter you want to use for
          bucketing.  This value must be passed to the call to
          experiment.variant as a keyword argument.  Defaults to "user_id".
        * **seed**: (Optional) Overrides the seed for this experiment.  If this
          is not set, `name` is used as the seed.
        * **newer_than**: (Optional) The earliest time that a bucketing
          resource can have been created by in UTC epoch seconds.  If set, you
          must pass the time, in UTC epoch seconds, when the resource that you
          are bucketing was created to the call to experiment.variant as the
          "created" parameter. For example, if you are bucketing based on
          user_id, created would be set to the time when a User account was
          created or when an LoID cookie was generated.
    """

    @classmethod
    def from_dict(  # pylint: disable=redefined-builtin
        cls, id: int, name: str, owner: str, version: int, config: Dict[Any, Any]
    ) -> R2Experiment:
        variants = config.get("variants", {})
        assert not set(variants.keys()) - {"active"}
        return super().from_dict(id=id, name=name, owner=owner, version=version, config=config)

    def should_log_bucketing(self) -> bool:
        return False
