Baseplate.py
============

It's much easier to manage a bunch of services when they all have the same
shape: the way they're developed, the way they interact with the infrastructure
they run on, and the way they interact with each other. Baseplate is reddit's
specification for the common shape of our services. This library, Baseplate.py,
is the Python implementation of that specification.

Baseplate.py integrates with existing application frameworks and provides
battle-tested libraries to give you everything you need to build a
well-behaving production service without having to reinvent the wheel.

Here's a simple Baseplate.py HTTP service built using the `Pyramid web
framework`_:

.. literalinclude:: tutorial/chapter4/sql.py
   :language: python

.. _`Pyramid web framework`: https://trypyramid.com/

Every request to this example service will automatically emit telemetry that
allows you to dig into how the service is performing under the hood:

* Timers for how long the whole request took and how long was spent talking to
  the database.
* Counters for the success/failure of the whole request and each query to the
  database.
* Distributed tracing spans (including carrying over trace metadata from
  upstream services and onwards to downstream ones).
* Reporting of stack traces to Sentry on crash.

*And you don't have to write any of that.*

**To get started,** :doc:`dive into the tutorial <tutorial/index>`. Or if you
need an API reference, look below.

Table of Contents
-----------------

.. toctree::
   :titlesonly:

   tutorial/index
   guide/index
   api/index
   cli/index
   lint/index

Appendix
--------

* :ref:`genindex`
* :ref:`modindex`
