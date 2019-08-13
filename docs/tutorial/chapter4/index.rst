Clients --- Talking to the outside world
========================================

In the previous chapter, we integrated Baseplate.py into our Pyramid
application and got some observers working to see how things were performing.
Most applications are not so self-contained though and have to talk to other
services to do their job. In this chapter, we'll add a dependency on a database
to see what that looks like.

Adding a database
-----------------

We're going to use a popular Python ORM called `SQLAlchemy`_ to talk to our
database. Let's install that to get started:

.. _`SQLAlchemy`: https://www.sqlalchemy.org/

.. code-block:: console

   $ pip install sqlalchemy

Now that's installed, we can use Baseplate.py's helpers to add SQLAlchemy to
our service.

.. literalinclude:: sql.py
   :language: python
   :emphasize-lines: 2,10,17

Pretty simple, but there's something subtle going on here. Let's dig into it.

.. literalinclude:: sql.py
   :language: python
   :start-at: configure_context
   :end-before: Configurator

This call to :py:meth:`~baseplate.Baseplate.configure_context`, during
application startup, tells Baseplate.py that we want to add a SQLAlchemy
:py:class:`~sqlalchemy.orm.session.Session` to the "context" with the name
``db``.

What exactly the "context" is depends on what framework you're using, but for
Pyramid applications it's the ``request`` object that Pyramid gives to every
request handler.

.. note::
   Why do we pass in the context configuration as a dictionary? It's possible
   to set up multiple clients at the same time this way. You can even do more
   complicated things like nesting dictionaries to organize the stuff you add
   to the context. See :py:meth:`~baseplate.Baseplate.configure_context` for
   more info.

.. literalinclude:: sql.py
   :language: python
   :start-at: request.db
   :end-at: request.db

Since we have connected the :py:class:`~baseplate.Baseplate` object with
Pyramid and told it to configure the context like this, we'll now see a ``db``
attribute on the ``request`` that has that SQLAlchemy session we wanted.

OK. Now we have got that wired up, let's try running it.

.. code-block:: console

   $ baseplate-serve --debug helloworld.ini
   ...
   baseplate.lib.config.ConfigurationError: db.url: no value specified

Ah! It looks like we have got some configuring to do.

Configure the new client
------------------------

Telling Baseplate.py that we wanted to add the SQLAlchemy session to our
context did not actually give it any hint about how that session should be
configured. SQLAlchemy can transparently handle different SQL databases for us
and the location at which to find them will be different depending on if we're
running in production, staging, or development. So it's time for the
configuration file again.

.. literalinclude:: sql.ini
   :language: ini
   :emphasize-lines: 6

To wire up the database, all we need is to add a SQLAlchemy
:py:class:`~sqlalchemy.engine.url.URL` to the configuration file. Because we
configured the session to use the name ``db`` the relevant configuration line
is prefixed with that name.

We're just going to use an in-memory `SQLite`_ database here because it's built
into Python and we don't have to install anything else.

.. _`SQLite`: https://www.sqlite.org/index.html

Now when we fire up the service, it launches and returns requests with our new
data.

.. code-block:: console

   $ curl localhost:9090
   {"Hello": "World", "Now": "2019-08-09"}

Great! If you look at the server logs when you make a request, you'll notice
there are new metrics:

.. code-block:: console
   :emphasize-lines: 3,5

   $ baseplate-serve --debug helloworld.ini
   ...
   17905:7296338476964580186:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.clients.db.execute:0.0824928|ms'
   17905:7296338476964580186:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.server.hello_world:4.16493|ms'
   17905:7296338476964580186:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.clients.db.execute.success:1|c'
   17905:7296338476964580186:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.server.hello_world.success:1|c'
   ...

The Baseplate.py SQLAlchemy integration automatically tracked usage of our
database and reports timers, counters, and other goodies to our monitoring
systems.

Summary
-------

We have now hooked up our service to a simple database and when we run queries
Baseplate.py automatically tracks them and emits telemetry.
