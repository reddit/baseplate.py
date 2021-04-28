Frequently Asked Questions
==========================

.. contents::
   :backlinks: none

Can I serve multiple protocols (Thrift, HTTP, etc.) in one service?
-------------------------------------------------------------------

Yes! While ``baseplate-serve`` doesn't support serving multiple protocols from
the same *process* it's totally fine to run multiple instances of
``baseplate-serve`` out of one code base. This allows you to present different
interfaces to different clients and scale each interface independently.

For example, our example HTTP service from :doc:`the tutorial
<../tutorial/index>` has an entrypoint function (``make_wsgi_app()``) that sets
up the application:

.. literalinclude:: ../tutorial/chapter4/helloworld.py
   :language: python
   :start-at: make_wsgi_app

using configuration for that application:

.. literalinclude:: ../tutorial/chapter4/helloworld.ini
   :language: ini
   :start-at: app:main
   :end-before: server:main

and configuration that tells ``baseplate-serve`` how to serve the application:

.. literalinclude:: ../tutorial/chapter4/helloworld.ini
   :language: ini
   :start-at: server:main

Note the ``factory`` setting in each section refers to a module and name in
that module. We can use these to point ``baseplate-serve`` at different pieces
of code.

To serve an additional protocol, we need another entrypoint function that
returns a different kind of application. Let's add a basic Thrift service as
well::

   def make_processor(app_config):
       baseplate = Baseplate(app_config)
       baseplate.configure_observers()
       baseplate.configure_context({"db": SQLAlchemySession()})

       handler = MyHandler()
       processor = MyService.Processor(handler)
       return baseplateify_processor(processor, logger, baseplate)

and add application configuration:

.. code-block:: ini

   [app:thrift]
   factory = helloworld:make_processor

   metrics.namespace = helloworld

   db.url = sqlite:///

and finally define server configuration:

.. code-block:: ini

   [server:thrift]
   factory = baseplate.server.thrift

We could now run our HTTP server with ``baseplate-serve myconfig.ini`` or our
Thrift server with ``baseplate-serve --server-name=thrift --app-name=thrift
myconfig.ini``. The ``--server-name`` and ``--app-name`` parameters tell
``baseplate-serve`` which sections of the config file to use.

There's something bad going on here though. Both server types are doing the
exact same stuff with :py:class:`~baseplate.Baseplate` setup and the
configuration for them is duplicated. A common pattern to clean this up is to
factor out a ``make_baseplate`` function and use it in both our entrypoints::

   def make_baseplate(app_config):
       baseplate = Baseplate(app_config)
       baseplate.configure_observers()
       baseplate.configure_context({"db": SQLAlchemySession()})


   def make_wsgi_app(app_config):
       baseplate = make_baseplate(app_config)

       ...


   def make_processor(app_config):
       baseplate = make_baseplate(app_config)

       ...

Similarly, you can factor out common configuration items into a ``[DEFAULT]``
section in the config file which will be automatically inherited by all other
sections:

.. code-block:: ini

   [DEFAULT]
   metrics.namespace = helloworld
   db.url = sqlite:///

   [app:main]
   factory = helloworld:make_wsgi_app

   [app:thrift]
   factory = baseplate.server.thrift

   ...

For more information on what's going on under the hood here, check out the
:doc:`startup`.


What do I do about "Metrics batch of N bytes is too large to send"?
-------------------------------------------------------------------

As your application processes a request, it does various actions that get
counted and timed. Baseplate.py batches up these metrics and sends them to the
metrics aggregator at the end of each request. The metrics are sent as a single
UDP datagram that has a finite maximum size (the exact amount depending on the
server) that is sufficiently large for normal purposes.

Seeing this error generally means that the application generated a *lot* of
metrics during the processing of that request. Since requests are meant to be
short lived, this indicates that the application is doing something
pathological in that request; a common example is making queries to a database
in a loop.

The best course of action is to dig into the application and reduce the amount
of work done in a given request by e.g. batching up those queries-in-a-loop
into fewer round trips to the database. This has the nice side-effect of
speeding up your application too!  To get you started, the "batch is too large"
error message also contains a list of the top counters in the oversized batch.
For example, if you see something like
``myservice.clients.foo_service.do_bar=9001`` that means you called the
``do_bar()`` method on ``foo_service`` over 9,000 times!

.. note:: For cron jobs or other non-server usages of Baseplate.py, you may
   need to break up your work into smaller units. For example, if your cron job
   processes a CSV file of 10,000 records you could create a server span for
   each record rather than one for the whole job.

Because this does not usually come up outside of legitimate performance issues
in the application, there are currently no plans to automatically flush very
large batches of metrics (which would silently mask performance issues like
this).


What do I do about "Context objects cannot be re-used"?
-------------------------------------------------------

This means that the application used the same context object, like one created
with :py:meth:`~baseplate.Baseplate.make_context_object`, in multiple calls to
:py:meth:`~baseplate.Baseplate.make_server_span`. Context objects are tied to a
single server span and cannot be re-used between different spans. Make sure to
create a new context object for each span. The
:py:meth:`~baseplate.Baseplate.server_context` helper can simplify this
lifecycle.

What do I do about "Cannot make child span of parent that already finished"?
----------------------------------------------------------------------------

This means that :py:meth:`~baseplate.Span.make_child` was called on a span that
has already finished. This usually happens when an application holds onto a
reference to a span and tries to continue doing operations with it after the
parent request finished.

This can happen because of the application spawned a number of greenlets to do
tasks concurrently but let the root greenlet finish up before the children did.
The safest thing to do here is to ensure that the parent properly waits for its
children before exiting.

Sometimes this is done deliberately to give a quick response to the client.
It's generally best to put background work like this into a separate dedicated
work queue as service worker processes are ephemeral and can be killed at any
time without warning.
