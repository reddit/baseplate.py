``baseplate.lib.config``
========================

.. automodule:: baseplate.lib.config

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
.. autofunction:: UnixUser
.. autofunction:: UnixGroup
.. autofunction:: OneOf
.. autofunction:: TupleOf

If you need something custom or fancy for your application, just use a
callable which takes a string and returns the parsed value or raises
:py:exc:`ValueError`.

Combining Types
---------------

These options are used in combination with other types to form more complex
configurations.

.. autofunction:: Optional
.. autofunction:: Fallback
.. autofunction:: DictOf

Data Types
----------

.. autoclass:: EndpointConfiguration

Add a new parser
----------------

.. autoclass:: Parser

Exceptions
----------

.. autoexception:: ConfigurationError
