``baseplate.crypto``
====================

.. automodule:: baseplate.crypto

Message Signing
---------------

.. autofunction:: make_signature

.. autofunction:: validate_signature

.. autoclass:: SignatureInfo

Exceptions
~~~~~~~~~~

.. autoexception:: SignatureError

.. autoexception:: UnreadableSignatureError

.. autoexception:: IncorrectSignatureError

.. autoexception:: ExpiredSignatureError


Utilities
---------

.. autofunction:: constant_time_compare
