from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import hashlib
import time

from .base import Experiment
from ..._compat import long, iteritems

from .variant_sets.single_variant_set import SingleVariantSet
from .variant_sets.multi_variant_set import MultiVariantSet
from .variant_sets.rollout_variant_set import RolloutVariantSet

logger = logging.getLogger(__name__)


variant_type_map = {
    'single_variant': SingleVariantSet,
    'multi_variant': MultiVariantSet,
    'feature_rollout': RolloutVariantSet,
}


class SimpleExperiment(Experiment):

    def make_seed(self, seed_data):
        id = seed_data.get('id')
        name = seed_data.get('name')
        version = seed_data.get('shuffle_version')

        return "{}.{}.{}".format(id, name, version)

    def __init__(self, id, name, owner, start_ts, stop_ts, config,
                 experiment_version, shuffle_version, variant_set,
                 bucket_seed, bucket_val, enabled=True, **kwargs):
        """
        :param int id -- the experiment id. This should be unique.
        :param string name -- the human-readable name of the experiment.
        :param string owner -- who is responsible for this experiement.
        :param timestamp start_ts -- when this experiment is due to start.
            Variant requests prior to this time will return None
        :param timestamp stop_ts -- when this experiment is due to end.
            Variant requests after this time will return None.
        :param dict config -- the configuration for this experiment.
        :param int experiment_version -- which version of this experiment is
            being used. This value should increment with each successive change
            to the experimental configuration
        :param int shuffle_version -- distinct from the experiment version, this
            value is used in constructing the default bucketing seed value (if not
            provided). When this value changes, rebucketing will occur.
        :param list variants -- the list of variants for this experiment. This should
            be provided as an array of dicts, each containing the keys 'name'
          and 'size'. Name is the variant name, and size is the fraction of
          users to bucket into the corresponding variant. Sizes are expressed
          as a floating point value between 0 and 1.
        :param bucket_seed -- if provided, this provides the seed for determining
            which bucket a variant request lands in. Providing a consistent
            bucket_seed will ensure a user is bucketed consistently.
        :param bool enabled -- whether or not this experiment is enabled.
            disabling an experiment means all variant calls will return None
        """

        self.id = id
        self.name = name
        self.owner = owner
        self.num_buckets = 1000

        self.start_ts = start_ts
        self.stop_ts = stop_ts
        self.enabled = enabled

        self.bucket_val = bucket_val
        self.version = experiment_version
        self.shuffle_version = shuffle_version
        self.experiment_version = experiment_version

        if not self.experiment_version:
            raise ValueError('Experiment version must be provided.')

        self.variant_set = variant_set

        seed_data = {"id": id, "name": name, "shuffle_version": self.shuffle_version}
        self._seed = bucket_seed or self.make_seed(seed_data)

    @classmethod
    def from_dict(cls, id, name, owner, start_ts, stop_ts, config,
                  variant_type, enabled=True, **kwargs):
        bucket_val = config.get("bucket_val", "user_id")
        version = config.get("experiment_version")
        shuffle_version = config.get("shuffle_version")

        variants = config.get("variants", [])

        variant_type_cls = variant_type_map.get(variant_type)

        if variant_type_cls is None:
            raise ValueError('Invalid experiment type: {}'.format(variant_type))

        variant_set = variant_type_cls(variants)

        bucket_seed = config.get("bucket_seed")

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
        )

    @property
    def seed(self):
        return self._seed

    def get_unique_id(self, **kwargs):
        if kwargs.get(self.bucket_val):
            return ":".join(
                [self.name, self.bucket_val, str(kwargs[self.bucket_val])]
            )
        else:
            return None

    def should_log_bucketing(self):
        """ Default to true. Override if logging of eligibility events not required. """
        return True

    def variant(self, **kwargs):
        lower_kwargs = {k.lower(): v for k, v in iteritems(kwargs)}

        if not self._is_enabled():
            return None

        if self.bucket_val not in lower_kwargs:
            logger.info(
                "Must specify %s in call to variant for experiment %s.",
                self.bucket_val,
                self.name,
            )
            return None

        if lower_kwargs[self.bucket_val] is None:
            logger.info(
                "Cannot choose a variant for bucket value %s = %s "
                "for experiment %s.",
                self.bucket_val,
                lower_kwargs[self.bucket_val],
                self.name,
            )
            return None

        bucket = self._calculate_bucket(lower_kwargs[self.bucket_val])
        return self._choose_variant(bucket)

    def _is_enabled(self, **kwargs):
        current_ts = time.time()

        return (self.enabled and current_ts >= self.start_ts
                and current_ts < self.stop_ts)

    def _calculate_bucket(self, bucket_val):
        """Sort something into one of self.num_buckets buckets.

        :param bucket_val -- a string used for shifting the deterministic bucketing
                       algorithm.  In most cases, this will be an Account's
                       _fullname.
        :return int -- a bucket, 0 <= bucket < self.num_buckets
        """
        # Mix the experiment seed with the bucket_val so the same users don't
        # get bucketed into the same bucket for each experiment.
        seed_bytes = ("%s%s" % (self.seed, bucket_val)).encode()
        hashed = hashlib.sha1(seed_bytes)
        bucket = long(hashed.hexdigest(), 16) % self.num_buckets
        return bucket

    def _choose_variant(self, bucket):
        return self.variant_set.choose_variant(bucket)
