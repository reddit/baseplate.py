from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import hashlib
import time

from .base import Experiment
from ..._compat import long, iteritems


logger = logging.getLogger(__name__)


class SimpleExperiment(Experiment):
    """Base class for simple experiments. Note that this class shares a lot of
    code from the r2.py class, which is slated for deprecation and removal.
    """

    def make_seed(self, seed_data):
        id = seed_data.get('id')
        name = seed_data.get('name')
        version = seed_data.get('version')

        return "{}.{}.{}".format(id, name, version)

    def __init__(self, id, name, owner, start_ts, stop_ts, config,
                 enabled=True, **kwargs):

        self.id = id
        self.name = name
        self.owner = owner
        self.num_buckets = 1000

        self.start_ts = start_ts
        self.stop_ts = stop_ts
        self.enabled = enabled

        self.bucket_val = config.get("bucket_val", "user_id")
        self.version = config.get("experiment_version")
        self.shuffle_version = config.get("shuffle_version")

        self.variants = config.get("variants", [])

        seed_data = {"id": id, "name": name, "version": self.shuffle_version}
        self.seed = config.get("bucket_seed", self.make_seed(seed_data))

        self._validate_variants(self.variants)

    def _get_seed(self):
        return self.seed

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

        current_ts = time.time()
        if (not self.enabled
                or current_ts < self.start_ts
                or current_ts > self.stop_ts):
            return None

        variant = self._check_overrides(**lower_kwargs)
        if variant is not None and variant in self.variants:
            return variant

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

        if not self._is_targeted(**lower_kwargs):
            return None

        bucket = self._calculate_bucket(lower_kwargs[self.bucket_val])
        return self._choose_variant(bucket)

    def _check_overrides(self, **kwargs):
        """Check if any of the kwargs override the variant. Functionality
        to be built in the future. For now, overrides are not supported.
        """
        return None

    def _is_targeted(self, **kwargs):
        """Check if user/etc is targeted for this experiment.

        Advanced targeting functionality to be supported in the future.
        For now, return True.
        """
        return True

    def _is_enabled(self, **kwargs):
        return self.start_ts

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

    def _validate_variants(self, variants):
        """ Validate that the variants, as provided, are valid for this type of experiment.
            For example, ensure that variant percentages do not add to more than 100%.
        """

        total_size = 0.0
        for variant in variants:
            total_size += variant.get('size')
        if total_size > 1.0:
            raise ValueError('Sum of all variants is greater than 100%')

        if not self.version:
            raise ValueError('Experiment version must be provided.')

    def _choose_variant(self, bucket):
        raise NotImplementedError
