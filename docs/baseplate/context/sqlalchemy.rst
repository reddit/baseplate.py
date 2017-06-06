``baseplate.context.sqlalchemy``
================================

.. automodule:: baseplate.context.sqlalchemy

Configuration Parsing
---------------------

.. function:: sqlalchemy.engine_from_config(configuration, prefix='sqlalchemy.', **kwargs)

   Make an engine from a configuration dictionary.

   The keys useful to :py:func:`~sqlalchemy.engine_from_config` should be
   prefixed, e.g. ``sqlalchemy.url`` etc. The ``prefix`` argument specifies the
   prefix used to filter keys. Each key is mapped to a corresponding keyword
   argument on :py:func:`~sqlalchemy.create_engine`. Any keyword arguments
   given to this function will be passed through. Keyword arguments take
   precedence over the configuration file.


Classes
-------

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemyEngineContextFactory

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemySessionContextFactory
