import hashlib
import logging

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from baseplate.lib.experiments.providers.base import Experiment


logger = logging.getLogger(__name__)


class R2Experiment(Experiment):
    """A "legacy", r2-style experiment.

    .. deprecated:: 0.27
       Use SimpleExperiment with SingleVariantSet or MultiVariantSet instead.

    Should log bucketing events to the event pipeline.

    Note that this style of experiment caps the size of your variants such
    that:

    .. code-block:: python

        def max_variant_size(variant_size, num_variants):
            return max(variant_size, (1/num_variants) * 100)

    The config dict is expected to have the following values:

        * **variants**: dict mapping variant names to their sizes. Variant
          sizes are expressed as numeric percentages rather than a fraction of
          1 (that is, 1.5 means 1.5%, not 150%).
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

    # pylint: disable=redefined-builtin
    def __init__(
        self,
        id: int,
        name: str,
        owner: str,
        variants: Dict[str, float],
        seed: Optional[str] = None,
        bucket_val: str = "user_id",
        targeting: Optional[Dict[str, List[Any]]] = None,
        overrides: Optional[Dict[str, Dict[Any, Any]]] = None,
        newer_than: Optional[float] = None,
        version: Optional[int] = None,
    ):
        targeting = dict(targeting or {})
        overrides = dict(overrides or {})
        self.targeting: Dict[str, List[Any]] = {}
        self.overrides: Dict[str, Dict[Any, Any]] = {}
        self._case_sensitive_overrides = [
            param_name.lower() for param_name in overrides.pop("__case_sensitive__", [])
        ]
        self._case_sensitive_targeting = [
            param_name.lower() for param_name in targeting.pop("__case_sensitive__", [])
        ]
        for param, value in targeting.items():
            assert isinstance(param, str)
            assert isinstance(value, list)
            # even if the targeting parameter is case sensitive, the paramer
            # name cannot be
            key = param.lower()
            self.targeting[param.lower()] = []
            is_case_sensitive = key in self._case_sensitive_targeting
            for v in value:
                if is_case_sensitive or not isinstance(v, str):
                    self.targeting[param.lower()].append(v)
                else:
                    self.targeting[param.lower()].append(v.lower())
        for param, override_value in overrides.items():
            assert isinstance(param, str)
            assert isinstance(override_value, dict)
            # even if the override parameter is case sensitive, the paramer
            # name cannot be
            key = param.lower()
            self.overrides[key] = {}
            is_case_sensitive = key in self._case_sensitive_overrides
            for k, v in override_value.items():
                if is_case_sensitive or not isinstance(k, str):
                    override_val = k
                else:
                    override_val = k.lower()
                self.overrides[key][override_val] = v
        self.id = id
        self.name = name
        self.owner = owner
        self.seed = seed if seed else name
        self.num_buckets = 1000
        self.variants = variants
        self.bucket_val = bucket_val
        self.newer_than = newer_than
        self.version = version

    # pylint: disable=redefined-builtin
    @classmethod
    def from_dict(
        cls, id: int, name: str, owner: str, version: int, config: Dict[Any, Any]
    ) -> "R2Experiment":
        """Parse the config dict and return a new R2Experiment object.

        :param id: The id of the experiment from the base config.
        :param name: The name of the experiment from the base config.
        :param owner: The owner of the experiment from the base config.
        :param config: The "experiment" config dict from the base config.
        """
        return cls(
            id=id,
            name=name,
            owner=owner,
            version=version,
            variants=config["variants"],
            targeting=config.get("targeting"),
            overrides=config.get("overrides"),
            seed=config.get("seed"),
            bucket_val=config.get("bucket_val", "user_id"),
            newer_than=config.get("newer_than"),
        )

    def get_unique_id(self, **kwargs: Any) -> Optional[str]:
        if kwargs.get(self.bucket_val):
            return ":".join([self.name, self.bucket_val, str(kwargs[self.bucket_val])])
        return None

    def should_log_bucketing(self) -> bool:
        return True

    def variant(self, **kwargs: Any) -> Optional[str]:
        lower_kwargs = {k.lower(): v for k, v in kwargs.items()}

        variant = self._check_overrides(**lower_kwargs)
        if variant is not None and variant in self.variants:
            return variant

        if self.bucket_val not in lower_kwargs:
            logger.info(
                "Must specify %s in call to variant for experiment %s.", self.bucket_val, self.name
            )
            return None
        if lower_kwargs[self.bucket_val] is None:
            logger.info(
                "Cannot choose a variant for bucket value %s = %s for experiment %s.",
                self.bucket_val,
                lower_kwargs[self.bucket_val],
                self.name,
            )
            return None

        if not self._is_enabled(**lower_kwargs):
            return None

        bucket = self._calculate_bucket(lower_kwargs[self.bucket_val])
        return self._choose_variant(bucket)

    def _check_overrides(self, **kwargs: Any) -> Optional[str]:
        """Check if any of the kwargs override the variant."""
        for override_arg in self.overrides:
            if override_arg in kwargs:
                values = kwargs[override_arg]
                if not isinstance(values, (list, tuple)):
                    values = [values]
                for value in values:
                    if not isinstance(value, str):
                        final_value = value
                    elif override_arg in self._case_sensitive_overrides:
                        final_value = value
                    else:
                        final_value = value.lower()
                    override = self.overrides[override_arg].get(final_value)
                    if override is not None:
                        return override
        return None

    def _is_enabled(self, **kwargs: Any) -> bool:
        """Check if the targeting parameters in kwargs allow us to perform the experiment."""
        for targeting_param, allowed_values in self.targeting.items():
            if targeting_param in kwargs:
                targeting_values = kwargs[targeting_param]
                if not isinstance(targeting_values, (list, tuple)):
                    targeting_values = [targeting_values]
                if not isinstance(allowed_values, list):
                    allowed_values = [allowed_values]
                for value in targeting_values:
                    if not isinstance(value, str):
                        final_value = value
                    elif targeting_param in self._case_sensitive_targeting:
                        final_value = value
                    else:
                        final_value = value.lower()
                    if final_value in allowed_values:
                        if targeting_param == "logged_in" and self.newer_than:
                            user_created = kwargs.get("user_created")
                            if user_created and user_created > self.newer_than:
                                return True

                        else:
                            return True
        return False

    def _calculate_bucket(self, bucket_val: str) -> int:
        """Sort something into one of self.num_buckets buckets.

        :param bucket_val: a string used for shifting the deterministic bucketing
                       algorithm.  In most cases, this will be an Account's
                       _fullname.
        :return: a bucket, 0 <= bucket < self.num_buckets
        """
        # Mix the experiment seed with the bucket_val so the same users don't
        # get bucketed into the same bucket for each experiment.
        seed_bytes = (f"{self.seed}{bucket_val}").encode()
        hashed = hashlib.sha1(seed_bytes)
        bucket = int(hashed.hexdigest(), 16) % self.num_buckets
        return bucket

    def _choose_variant(self, bucket: int) -> Optional[str]:
        """Deterministically choose a percentage-based variant.

        The algorithm satisfies two conditions:

        1. It's deterministic (that is, every call with the same bucket and
           variants will result in the same answer).
        2. An increase in any of the variant percentages will keep the same
           buckets in the same variants as at the smaller percentage (that is,
           all buckets previously put in variant A will still be in variant A,
           all buckets previously put in variant B will still be in variant B,
           etc. and the increased percentages will be made of up buckets
           previously not assigned to a bucket).

        These attributes make it suitable for use in A/B experiments that may
        see an increase in their variant percentages post-enabling.

        :param bucket: an integer bucket representation
        :param variants: a dictionary of
                           <string:variant name>:<float:percentage> pairs.  If
                           any percentage exceeds 1/n percent, where n is the
                           number of variants, the percentage will be capped to
                           1/n.  These variants will be added to
                           DEFAULT_CONTROL_GROUPS to create the effective
                           variant set.
        :returns: the variant name, or None if bucket doesn't fall into any of
            the variants
        """
        # Say we have an experiment with two new things we're trying out for 2%
        # of users (A and B), a control group with 5% (C), and a pool of
        # excluded users (x).  The buckets will be assigned like so:
        #
        #     A B C A B C x x C x x C x x C x x x x x x x x x...
        #
        # This scheme allows us to later increase the size of A and B to 7%
        # while keeping the experience consistent for users in any group other
        # than excluded users:
        #
        #     A B C A B C A B C A B C A B C A B x A B x x x x...
        #
        # Rather than building this entire structure out in memory, we can use
        # a little bit of math to figure out just the one bucket's value.
        num_variants = len(self.variants)
        variant_names = sorted(self.variants.keys())
        # If the variants took up the entire set of buckets, which bucket would
        # we be in?
        candidate_variant = variant_names[bucket % num_variants]
        # Log a warning if this variant is capped, to help us prevent user (us)
        # error.  It's not the most correct to only check the one, but it's
        # easy and quick, and anything with that high a percentage should be
        # selected quite often.
        variant_fraction = self.variants[candidate_variant] / 100.0
        variant_cap = 1.0 / num_variants
        if variant_fraction > variant_cap:
            logger.warning(
                "Variant %s exceeds allowable percentage (%.2f > %.2f)",
                candidate_variant,
                variant_fraction,
                variant_cap,
            )
        # Variant percentages are expressed as numeric percentages rather than
        # a fraction of 1 (that is, 1.5 means 1.5%, not 150%); thus, at 100
        # buckets, buckets and percents map 1:1 with each other.  Since we may
        # have more than 100 buckets (causing each bucket to represent less
        # than 1% each), we need to scale up how far "right" we move for each
        # variant percent.
        bucket_multiplier = self.num_buckets / 100
        # Now check to see if we're far enough left to be included in the
        # variant percentage.
        bucket_limit = self.variants[candidate_variant] * num_variants * bucket_multiplier
        if bucket < int(bucket_limit):
            return candidate_variant
        return None
