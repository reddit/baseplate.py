baseplate.py
============

|Build Status|

It's much easier to manage a bunch of services when they all have the same
shape: how they're developed, how they interact with the infrastructure, and
how they interact with eachother. Baseplate is reddit's specification for the
common shape of our services, and this library, Baseplate.py, is the Python
implementation of that specification.

Baseplate.py glues together tooling for interacting with the reddit backend
ecosystem and spackles over things that are missing. It integrates with Apache
Thrift, Pyramid, and client libraries for many systems to transparently make
your applications observable.

Baseplate applications transparently get:

* Timing and request rate metrics using statsd
* Distributed tracing with Zipkin
* Error reporting and aggregation with Sentry

And can take advantage of:

* Integration with commonly used clients like: Thrift, SQLAlchemy,
  cassandra-driver, pymemcache, redis-py, and Kombu
* An experiments framework for doing A/B tests
* Secrets securely pulled from Vault

And many other things! Read the `full docs
<https://baseplate.readthedocs.io/en/stable/>`__.

Baseplate.py requires Python 3.6 or newer.

.. |Build Status| image:: https://cloud.drone.io/api/badges/reddit/baseplate.py/status.svg
   :target: https://cloud.drone.io/reddit/baseplate.py
