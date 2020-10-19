import hashlib
import logging
import time

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

from baseplate.lib.experiments.providers.base import Experiment
from baseplate.lib.experiments.targeting.base import Targeting
from baseplate.lib.experiments.targeting.tree_targeting import create_targeting_tree
from baseplate.lib.experiments.variant_sets.base import VariantSet
from baseplate.lib.experiments.variant_sets.multi_variant_set import MultiVariantSet
from baseplate.lib.experiments.variant_sets.range_variant_set import RangeVariantSet
from baseplate.lib.experiments.variant_sets.rollout_variant_set import RolloutVariantSet
from baseplate.lib.experiments.variant_sets.single_variant_set import SingleVariantSet


logger = logging.getLogger(__name__)


variant_type_map: Dict[str, Type[VariantSet]] = {
    "single_variant": SingleVariantSet,
    "multi_variant": MultiVariantSet,
    "feature_rollout": RolloutVariantSet,
    "range_variant": RangeVariantSet,
}


def _generate_overrides(
    override_config: Optional[List[Dict[str, Any]]]
) -> Optional[List[Dict[str, Targeting]]]:
    """Generate a dictionary of overrides.

    The format of the override config is expected to be a list of dicts,
    where each dict contains a single key (the treatment to assign) and value
    (the targeting tree for that treatment).

    This combination is used to ensure that the ordering of the overrides is
    maintained, allowing consistent overriding for any overlapping area
    across targeting trees. Each incoming variant request will check each
    targeting override in sequence until one is matched or none match.

    In the event of an invalid override:
    1) if a dict of {variant: target} is not provided, that list item will be
        ignored. All other overrides will still remain active.
    2) if the targeting object is invalid, the override will remain active,
        but will return false for all targeting

    Multiple targets can be provided for a single variant.
    """
    override_list = []

    if not override_config:
        return None

    if not isinstance(override_config, list):
        logger.error("Invalid override configuration. Skipping overrides.")
        return None

    for override_entry in override_config:
        if not isinstance(override_entry, dict) or len(override_entry) > 1:
            logger.error(
                "Invalid override configuration. Not applying override. "
                "Expected dictionary with single entry: %s",
                override_entry,
            )
            continue

        for treatment, targeting_tree_cfg in override_entry.items():
            targeting_tree = _generate_targeting(targeting_tree_cfg)
            override_list.append({treatment: targeting_tree})

    return override_list


def _generate_targeting(targeting_config: Optional[Dict[str, Any]]) -> Targeting:
    """Generate the targeting tree for this experiment.

    If no config is provided, then assume we want to target all. If an invalid
    config is provided, then target none.
    """
    if targeting_config is None:
        return create_targeting_tree({"OVERRIDE": True})

    try:
        return create_targeting_tree(targeting_config)
    except Exception:
        logger.error("Unable to create targeting tree. No targeting applied.")
        return create_targeting_tree({"OVERRIDE": False})


class SimpleExperiment(Experiment):
    """A basic experiment choosing from a set of variants.

    Simple experiments are meant to be used in conjunction with a VariantSet.
    This class serves as the replacement for the legacy r2 and feature_flag
    providers.

    :param id: The experiment id. This should be unique.
    :param name: The human-readable name of the experiment.
    :param owner: Who is responsible for this experiement.
    :param start_ts: When this experiment is due to start.
        Variant requests prior to this time will return None. Expects
        timestamp in seconds.
    :param stop_ts: When this experiment is due to end.
        Variant requests after this time will return None. Expects
        timestamp in seconds.
    :param config: The configuration for this experiment.
    :param experiment_version: Which version of this experiment is
        being used. This value should increment with each successive change
        to the experimental configuration.
    :param shuffle_version: Distinct from the experiment version, this
        value is used in constructing the default bucketing seed value (if not
        provided). When this value changes, rebucketing will occur.
    :param variants: The list of variants for this experiment. This should
        be provided as an array of dicts, each containing the keys 'name'
        and 'size'. Name is the variant name, and size is the fraction of
        users to bucket into the corresponding variant. Sizes are expressed
        as a floating point value between 0 and 1.
    :param bucket_seed: If provided, this provides the seed for determining
        which bucket a variant request lands in. Providing a consistent
        bucket_seed will ensure a user is bucketed consistently. Calls to
        the variant method will return consistent results for any given seed.
    :param enabled: Whether or not this experiment is enabled.
        disabling an experiment means all variant calls will return None.
    :param log_bucketing: Whether or not to log bucketing events.
    :param num_buckets: How many available buckets there are for
        bucketing requests. This should match the num_buckets in the
        provided VariantSet. The default value is 1000, which provides
        a potential variant granularity of 0.1%.

    """

    # pylint: disable=redefined-builtin, unused-argument
    def __init__(
        self,
        id: int,
        name: str,
        owner: str,
        start_ts: int,
        stop_ts: int,
        config: Dict[str, Any],
        experiment_version: int,
        shuffle_version: Optional[int],
        variant_set: VariantSet,
        bucket_seed: Optional[str],
        bucket_val: str,
        targeting: Targeting,
        overrides: Optional[List[Dict[str, Targeting]]],
        enabled: bool = True,
        log_bucketing: bool = True,
        num_buckets: int = 1000,
    ):
        self.id = id
        self.name = name
        self.owner = owner
        self.num_buckets = num_buckets

        self.start_ts = start_ts
        self.stop_ts = stop_ts
        self.enabled = enabled

        self.bucket_val = bucket_val
        self.version = experiment_version
        self.shuffle_version = shuffle_version
        self.experiment_version = experiment_version

        self._log_bucketing = log_bucketing

        self._targeting = targeting
        self._overrides = overrides

        if not self.experiment_version:
            raise ValueError("Experiment version must be provided.")

        self.variant_set = variant_set

        if bucket_seed:
            self._seed = bucket_seed
        else:
            self._seed = f"{id}.{name}.{shuffle_version}"

    @classmethod
    def from_dict(
        cls,
        id: int,
        name: str,
        owner: str,
        start_ts: int,
        stop_ts: int,
        config: Dict[str, Any],
        variant_type: str,
        enabled: bool = True,
    ) -> "SimpleExperiment":
        bucket_val = config.get("bucket_val", "user_id")
        version = config["experiment_version"]
        shuffle_version = config.get("shuffle_version")
        num_buckets = config.get("num_buckets", 1000)

        variants = config.get("variants", [])

        variant_type_cls = variant_type_map.get(variant_type)

        if variant_type_cls is None:
            raise ValueError(f"Invalid experiment type: {variant_type}")

        variant_set = variant_type_cls(variants, num_buckets=num_buckets)

        log_bucketing = config.get("log_bucketing", True)

        bucket_seed = config.get("bucket_seed")

        targeting_config: Optional[Dict[str, Any]] = config.get("targeting")
        targeting = _generate_targeting(targeting_config)

        override_config: Optional[List[Dict[str, Any]]] = config.get("overrides")
        overrides = _generate_overrides(override_config)

        return cls(
            id=id,
            name=name,
            owner=owner,
            start_ts=start_ts,
            stop_ts=stop_ts,
            enabled=enabled,
            config=config,
            experiment_version=version,
            shuffle_version=shuffle_version,
            variant_set=variant_set,
            bucket_seed=bucket_seed,
            bucket_val=bucket_val,
            num_buckets=num_buckets,
            log_bucketing=log_bucketing,
            targeting=targeting,
            overrides=overrides,
        )

    @property
    def seed(self) -> str:
        return self._seed

    def get_unique_id(self, **kwargs: Any) -> Optional[str]:
        if kwargs.get(self.bucket_val):
            return ":".join([self.name, self.bucket_val, str(kwargs[self.bucket_val])])
        return None

    def should_log_bucketing(self) -> bool:
        """Whether or not this experiment should log bucketing events."""
        return self._log_bucketing

    def is_targeted(self, **kwargs: Any) -> bool:
        """Return if kwargs match targeting parameters for this experiment."""
        return self._targeting.evaluate(**kwargs)

    def get_override(self, **kwargs: Any) -> Optional[str]:
        """Return if kwargs match targeting parameters for forcing a particular variant."""
        if not self._overrides:
            return None

        for override_node in self._overrides:
            for variant, targeting in override_node.items():
                if targeting.evaluate(**kwargs):
                    return variant

        return None

    def variant(self, **kwargs: Any) -> Optional[str]:
        if not self._is_enabled():
            return None

        lower_kwargs = {k.lower(): v for k, v in kwargs.items()}

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

        override = self.get_override(**kwargs)
        if override:
            return override

        if not self.is_targeted(**kwargs):
            return None

        bucket = self._calculate_bucket(lower_kwargs[self.bucket_val])
        return self._choose_variant(bucket)

    def _is_enabled(self, **_kwargs: Any) -> bool:
        current_ts = time.time()
        return self.enabled and self.start_ts <= current_ts < self.stop_ts

    def _calculate_bucket(self, bucket_val: str) -> int:
        """Sort something into one of self.num_buckets buckets.

        :param bucket_val: a string used for shifting the deterministic
            bucketing algorithm.  In most cases, this will be an Account's
            _fullname.
        :return: a bucket, 0 <= bucket < self.num_buckets
        """
        # Mix the experiment seed with the bucket_val so the same users don't
        # get placed into the same bucket for each experiment.
        seed_bytes = (f"{self.seed}{bucket_val}").encode()
        hashed = hashlib.sha1(seed_bytes)
        bucket = int(hashed.hexdigest(), 16) % self.num_buckets
        return bucket

    def _choose_variant(self, bucket: int) -> Optional[str]:
        return self.variant_set.choose_variant(bucket)
