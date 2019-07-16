``baseplate.lib.experiments``
===============================

.. automodule:: baseplate.lib.experiments

Experiment Providers
--------------------

.. toctree::
   :titlesonly:

   baseplate.lib.experiments.providers.r2: Legacy, R2-style experiments <r2>
   baseplate.lib.experiments.providers.feature_flag: Feature Flag experiments <feature_flag>
   baseplate.lib.experiments.providers.forced_variant: Forced Variant experiment <forced_variant>
   baseplate.lib.experiments.providers.simple_experiment: Simple experiment <simple_experiment>

Configuration Parsing
---------------------

.. autofunction:: experiments_client_from_config

.. autofunction:: baseplate.lib.experiments.providers.parse_experiment

Classes
-------

.. autoclass:: ExperimentsContextFactory

.. autoclass:: Experiments
   :members:
