``baseplate.config``
====================

.. automodule:: baseplate.config

Parser
------

.. autofunction:: parse_config

Value Types
-----------

Each option can have a type specified. Some types compose with other types to
make complicated expressions.

.. autofunction:: String
.. autofunction:: Float
.. autofunction:: Integer
.. autofunction:: Boolean
.. autofunction:: Endpoint
.. autofunction:: Timespan
.. autofunction:: Base64
.. autofunction:: File
.. autofunction:: Percent
.. autofunction:: OneOf
.. autofunction:: TupleOf
.. autofunction:: Optional
.. autofunction:: Fallback

If you need something custom or fancy for your application, just use a
callable which takes a string and returns the parsed value or raises
:py:exc:`ValueError`.

Data Types
----------

.. autoclass:: EndpointConfiguration

Exceptions
----------

.. autoexception:: ConfigurationError
