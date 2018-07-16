``baseplate.experiments``
===============================

.. automodule:: baseplate.experiments

Experiment Providers
--------------------

.. toctree::
   :titlesonly:

   baseplate.experiments.providers.r2: Legacy, R2-style experiments <r2>
   baseplate.experiments.providers.feature_flag: Feature Flag experiments <feature_flag>
   baseplate.experiments.providers.forced_variant: Forced Variant experiment <forced_variant>
   baseplate.experiments.providers.simple_experiment: Simple experiment <simple_experiment>

Configuration Parsing
---------------------

.. autofunction:: baseplate.experiments.experiments_client_from_config

.. autofunction:: baseplate.experiments.providers.parse_experiment

Classes
-------

.. autoclass:: baseplate.experiments.ExperimentsContextFactory

.. autoclass:: baseplate.experiments.Experiments
   :members:
