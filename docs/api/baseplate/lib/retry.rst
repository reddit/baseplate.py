``baseplate.lib.retry``
=======================

.. note:: This module is a low-level helper, many client libraries have
   protocol-aware retry logic built in. Check your library before using this.

.. automodule:: baseplate.lib.retry

.. autoclass:: RetryPolicy
   :members: new, yield_attempts, __iter__
