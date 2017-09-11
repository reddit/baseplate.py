from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import time

from datetime import datetime

from .feature_flag import FeatureFlag
from .forced_variant import ForcedVariantExperiment
from .r2 import R2Experiment
from ..._utils import warn_deprecated

logger = logging.getLogger(__name__)


ISO_DATE_FMT = "%Y-%m-%d"


def parse_experiment(config):
    """Factory method that parses an experiment config dict and returns an
    appropriate Experiment class.

    The config dict is expected to have the following values:

        * **id**: Integer experiment ID, should be unique for each experiment.
        * **name**: String experiment name, should be unique for each
          experiment.
        * **owner**: The group or individual that owns this experiment.
        * **version**: String to identify the specific version of the
          experiment.
        * **start_ts**: A float of seconds since the epoch of date and time
          when you want the experiment to start.  If an experiment has not been
          started yet, it is considered disabled.
        * **stop_ts**: A float of seconds since the epoch of date and time when
          you want the experiment to stop.  Once an experiment is stopped, it
          is considered disabled.
        * **type**: String specifying the type of experiment to run.  If this
          value is not recognized, the experiment will be considered disabled.
        * **experiment**: The experiment config dict for the specific type of
          experiment.  The format of this is determined by the specific
          experiment type.
        * **enabled**:  (Optional) If set to False, the experiment will be
          disabled and calls to experiment.variant will always return None and
          will not log bucketing events to the event pipeline. Defaults to
          True.
        * **global_override**: (Optional) If this is set, calls to
          experiment.variant will always return the override value and will not
          log bucketing events to the event pipeline.

    :param dict config: Configuration dict for the experiment you wish to run.
    :rtype: :py:class:`baseplate.experiments.providers.base.Experiment`
    :return: A subclass of :py:class:`Experiment` for the given experiment
        type.
    """
    experiment_type = config["type"].lower()
    experiment_id = config["id"]
    assert isinstance(experiment_id, int)
    name = config["name"]
    owner = config.get("owner")
    if "start_ts" in config and "stop_ts" in config:
        start_ts = config["start_ts"]
        stop_ts = config["stop_ts"]
    elif "expires" in config:
        warn_deprecated(
            "The 'expires' field is in experiment %s deprecated, you should "
            "use 'start_ts' and 'stop_ts'." % name
        )
        start_ts = time.time()
        expires = datetime.strptime(config["expires"], ISO_DATE_FMT)
        epoch = datetime(1970, 1, 1)
        stop_ts = (expires - epoch).total_seconds()
    else:
        raise ValueError(
            "Invalid config for experiment %s, missing start_ts and/or "
            "stop_ts." % name
        )

    if "version" in config:
        version = config["version"]
    else:
        warn_deprecated(
            "The 'version' field is not in experiment %s.  This field will be "
            "required in the future." % name
        )
        version = None
    now = time.time()
    if now < start_ts or now > stop_ts:
        return ForcedVariantExperiment(None)

    enabled = config.get("enabled", True)
    if not enabled:
        return ForcedVariantExperiment(None)

    experiment_config = config["experiment"]

    if "global_override" in config:
        # We want to check if "global_override" is in config rather than
        # checking config.get("global_override") because global_override = None
        # is a valid setting.
        override = config.get("global_override")
        return ForcedVariantExperiment(override)

    if experiment_type == "r2":
        return R2Experiment.from_dict(
            id=experiment_id,
            name=name,
            owner=owner,
            version=version,
            config=experiment_config,
        )
    elif experiment_type == "feature_flag":
        return FeatureFlag.from_dict(
            id=experiment_id,
            name=name,
            owner=owner,
            version=version,
            config=experiment_config,
        )
    else:
        logger.warning(
            "Found an experiment <%s:%s> with an unknown experiment type <%s> "
            "that is owned by <%s>. Please clean up.",
            experiment_id,
            name,
            experiment_type,
            owner,
        )
        return ForcedVariantExperiment(None)
