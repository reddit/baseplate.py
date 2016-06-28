from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
#from __future__ import unicode_literals This breaks __all__ on PY2

from . import config, metrics
from .core import Baseplate


def make_metrics_client(raw_config):
    """Configure and return a metrics client.

    This expects two configuration options:

    ``metrics.namespace``
        The root key to namespace all metrics in this application under.
    ``metrics.endpoint``
        A ``host:port`` pair, e.g. ``localhost:2014``. If an empty string, a
        client that discards all metrics will be returned.

    :param dict raw_config: The app configuration which should have settings
        for the metrics client.
    :return: A configured client.
    :rtype: :py:class:`baseplate.metrics.Client`

    """
    cfg = config.parse_config(raw_config, {
        "metrics": {
            "namespace": config.String,
            "endpoint": config.Optional(config.Endpoint),
        },
    })

    # pylint: disable=no-member
    return metrics.make_client(cfg.metrics.namespace, cfg.metrics.endpoint)


__all__ = [
    "make_metrics_client",
    "Baseplate",
]
