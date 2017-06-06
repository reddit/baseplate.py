``baseplate-serve``
===================

Baseplate comes with a simple Gevent-based server for both Thrift and WSGI
applications called ``baseplate-serve2`` or ``baseplate-serve3`` depending
on which version of Python you would like to use.

Configuration
-------------

There is one required parameter on the command line, the path to an INI-format
configuration file. There should be two sections in the file: the ``server``
section and the ``app`` section. The section headers look like ``server:main``
or ``app:main`` where the part before the ``:`` is the type of section and the
part after is the "name". Baseplate looks for sections named ``main`` by
default but can be overridden with the ``--server-name`` and ``--app-name``
options.

.. highlight:: ini

The Server
----------

Here's an example of a ``server`` section::

   [server:main]
   factory = baseplate.server.thrift
   stop_timeout = 30

The ``factory`` tells baseplate what code to use to run the server. Baseplate
comes with two servers built in:

``baseplate.server.thrift``
   A Gevent Thrift server.

``baseplate.server.wsgi``
   A Gevent WSGI server.

Both take two optional configuration values as well:

``max_concurrency``
   The maximum number of simultaneous clients the server will handle. Unlimited
   by default.

``stop_timeout``
   How long, in seconds, to wait for active connections to finish up gracefully
   when shutting down. By default, the server will shut down immediately.

The WSGI server takes an additional optional parameter:

``handler``
   A full name of a class which subclasses
   ``gevent.pywsgi.WSGIHandler`` for extra functionality.


The Application
---------------

And now the real bread and butter, your ``app`` section::

   [app:main]
   factory = my_app.processor:make_processor
   foo = 3
   bar = 22
   noodles.blah = one, two, three

The ``app`` section also takes a ``factory``.  This should be the name of a
callable in your code which builds and returns your application. The part
before the ``:`` is a Python module. The part after the ``:`` is the name of a
callable object within that module.

The rest of the options in the ``app`` section of the configuration file get
passed as a dictionary to your application callable. You can parse these
options with :py:mod:`baseplate.config`.

The application factory should return an appropriate object for your server:

Thrift
   A ``TProcessor``.

WSGI
   A WSGI callable.

Logging
-------

The baseplate server provides a default configuration for the Python standard
``logging`` system. The root logger will print to ``stdout`` with a format that
includes trace information. The default log level is ``INFO`` or ``DEBUG`` if
the ``--debug`` flag is passed to ``baseplate-serve``.

If more complex logging configuration is necessary, the configuration file will
override the default setup. The `configuration format`_ is documented in the
standard library.

.. _configuration format: https://docs.python.org/2/library/logging.config.html#logging-config-fileformat

Automatic reload on source changes
----------------------------------

In development, it's useful for the server to restart itself when you change
code.  You can do this by passing the ``--reload`` flag to ``baseplate-serve``.

This should not be used in production environments.

Einhorn
-------

``baseplate-serve`` can run as a worker in `Stripe's Einhorn socket manager`_.
This allows Einhorn to handle binding the socket, worker management, rolling
restarts, and worker health checks.

Baseplate supports Einhorn's "manual ACK" protocol. Once the application is
loaded and ready to serve, Baseplate notifies the Einhorn master process via
its command socket.

An example command line::

   einhorn -m manual -n 4 --bind localhost:9190 \
      baseplate-serve2 myapp.ini

.. _Stripe's Einhorn socket manager: https://github.com/stripe/einhorn

Debug Signal
------------

Applications running under ``baseplate-serve`` will respond to ``SIGUSR1`` by
printing a stack trace to the logger. This can be useful for debugging
deadlocks and other issues.

Note that Einhorn will exit if you send it a ``SIGUSR1``. You can instead open up
``einhornsh`` and instruct the master to send the signal to all workers::

   $ einhornsh
   > signal SIGUSR1
   Successfully sent USR1s to 4 processes: [...]
