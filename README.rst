baseplate
=========

|Build Status|

Baseplate is a library to build services on. Its goal is to provide all
the common things a service needs with as few surprises as possible,
including:

-  compatibility with Python 3.6+
-  transparent diagnostic information collection (metrics, tracing,
   logging)
-  configuration parsing
-  gevent-based Thrift and WSGI servers meant to run under
   `Einhorn <https://github.com/stripe/einhorn>`__
-  and various helper libraries like a Thrift client pool

Read the `full docs <https://baseplate.readthedocs.io/en/stable/>`__.

.. |Build Status| image:: https://cloud.drone.io/api/badges/reddit/baseplate.py/status.svg
   :target: https://cloud.drone.io/reddit/baseplate.py
