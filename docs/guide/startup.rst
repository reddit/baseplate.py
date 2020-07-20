Application Startup Sequence
============================

The most common way to start up a Baseplate.py application is to run one of the
the :doc:`../cli/serve` script. This page explains exactly what's going between
that command and your application.

.. contents::
   :backlinks: none

.. note::
   ``baseplate-script`` is another way to run code in a Baseplate.py
   application that is generally useful for ephemeral jobs like periodic crons
   or ad hoc tasks like migrations. That script follows a much abbreviated form
   of this sequence.

The Python Interpreter
----------------------

Because this is a Python application, before any code in Baseplate or your
application can run, the Python interpreter itself must set itself up.

There are many many many steps involved in :pep:`Python's startup sequence
<432>` but for our purposes the most important thing to highlight are a number
of `environment variables`_ that can configure the interpreter.

Now that the interpeter is up, it runs the actual program we wanted it to
(``baseplate-serve``) and the Baseplate.py startup sequence begins.

.. _`environment variables`: https://docs.python.org/3/using/cmdline.html#environment-variables

Gevent Monkeypatching
---------------------

Before doing anything else, Baseplate.py `monkeypatches`_ the standard library
to use `Gevent`_, a library that transparently makes Python asynchronous.  This
allows us to simulate simultaneously processing many requests by interleaving
their work and switching the CPU between them as they wait for IO operations
like network requests.  Monkeypatching replaces most of the APIs in the Python
standard library that can block a process with ones provided by Gevent which
take advantage of the blocking to swap to other work.

.. warning:: While Gevent gives us easy concurrency, it does *not* give us
   parallelism. Python is still only fundamentally processing these requests in
   one thread, one task at a time. Keep an eye out for code that would not
   yield to other tasks, like CPU-bound loops, APIs that don't have
   asynchronous equivalents (like :py:func:`~fcntl.flock`), or dropping into
   gevent-unaware native extensions. See the ``blocked_hub`` monitor in
   :ref:`runtime-metrics` for a tool that can help debug this class of problem.

Monkeypatching is done as early as possible in the process to ensure that all
other parts of the startup sequence use the monkeypatched IO primitives.

For more details on Gevent and how it works, see `gevent For the Working Python
Developer`_.

.. _`monkeypatches`: https://en.wikipedia.org/wiki/Monkey_patch
.. _`Gevent`: https://www.gevent.org/
.. _`gevent for the Working Python Developer`: https://sdiehl.github.io/gevent-tutorial/

Extending the ``PYTHONPATH``
----------------------------

Python uses a list of directories, sourced from the environment variable
``PYTHONPATH``, to search for libraries when doing imports. Because it's common
to want to run applications from the current directory, Baseplate.py adds the
current directory to the front of the path.

Listening for signals
---------------------

Baseplate.py registers some handlers for :py:mod:`signals <signal>` that allow
the outside system to interact with it once running. The following signals have
handlers defined:

:py:attr:`~signal.SIGUSR1`
   Dump a stack trace to ``stdout``. This can be useful for debugging if the
   process is not responsive.

:py:attr:`~signal.SIGTERM`
   Initiate graceful shutdown. The server will stop accepting new requests and
   shut down as soon as all currently in-flight requests are processed, or a
   timeout occurs.

:py:attr:`~signal.SIGUSR2`
   Same as :py:attr:`~signal.SIGTERM`. For use with Einhorn.

:py:attr:`~signal.SIGINT`
   Same as :py:attr:`~signal.SIGTERM`. For Ctrl-C on the command line.

Parsing command line arguments
------------------------------

Command line arguments are parsed using the Python-standard :py:mod:`argparse`
machinery.

``baseplate-serve`` only requires one argument: a path to the configuration
file for your service. The optional arguments ``--app-name`` and
``--server-name`` control which sections of the config file are read. The
remaining options control the way the server runs.

Parsing the configuration file
------------------------------

Baseplate.py loads the configuration file from the path given in command line.
The raw file on disk is parsed using a :py:class:`configparser.ConfigParser`
with interpolation disabled.

Configuration files are split up into sections that allow for one file to hold
configuration for multiple components. There are generally two types of section
in the config file: application configuration sections that look like
``[app:foo]`` and server configuration sections that look like
``[server:bar]``. After parsing the configuration file, Baseplate.py uses the
section names specified in the ``--app-name`` and ``--server-name`` command
line arguments to determine which sections to pay attention to. If not
specified on the command line, the default section name is ``main``. For
example, ``baseplate-serve --app-name=foo`` would load the ``[app:foo]`` and
``[server:main]`` sections from the config file.

.. note:: If you use multiple ``app`` or ``server`` blocks you may find
   yourself with a lot of repetition.  You can move duplicated configuration to
   a meta-section called ``[DEFAULT]`` and it will automatically be inherited
   in all other sections in the file (unless overridden locally).

The server configuration section is used to determine which server
implementation to use and then the rest of the configuration is passed onto
that server for instantiation.  See :ref:`server` for more details.  The
application configuration section determines how to load your application and
then the rest of the configuration is passed onto your code, see the
:ref:`load-your-code` section for more details.

Configuring Logging
-------------------

Next up, Baseplate.py configures Python's :py:mod:`logging` system. The default
configuration is:

* Logs are written to ``stdout``.
* The default log level is :py:attr:`~logging.INFO` unless the ``--debug``
  command line argument was passed which changes the log level to
  :py:attr:`~logging.DEBUG`.
* A baseline structured logging format is applied to log messages, see
  :doc:`the logging observer's documentation
  <../api/baseplate/observers/logging>` for details.

This configuration affects all messages emitted through ``logging`` (but not
e.g. :py:func:`print` calls).

If a ``[loggers]`` section is present in your configuration file, ``logging``
is given a chance to override configuration using the :ref:`standard logging
config file format <logging-config-fileformat>`. This can be useful if you want
finer grain control of what messages get filtered out etc.

.. _load-your-code:

Loading your code
-----------------

The next step is to load up your application code.

Baseplate.py looks inside the selected ``[app:foo]`` section for a setting
named ``factory``. The value of this setting should be the full name of a
callable, like ``my.module:my_callable`` where the part before the colon is a
module to import and the part after is a name within that module.  The
referenced module is imported with :py:func:`importlib.import_module` and then
the referenced name is retrieved with :py:func:`getattr` on that module object.

Once the callable is loaded, Baseplate.py passes in the parsed settings from
the selected ``[app:foo]`` section and waits for the function to return an
application object. This is where your application can do all of its one-time
startup logic outside of request processing.

Binding listening sockets
-------------------------

Unless running under Einhorn, Baseplate.py needs to create and bind a socket
for the server to listen on. The address bound to is selected by the ``--bind``
option and defaults to ``127.0.0.1:9090``.

Two socket options are applied when binding a socket:

``SO_REUSEADDR``
   This allows us to bind the socket even when connections from previous
   incarnations are still lingering in ``TIME_WAIT`` state.

``SO_REUSEPORT``
   This allows multiple instances of our application to bind to the same socket
   and the kernel distributes connections to them according to a deterministic
   algorithm. See `this explanation of SO_REUSEPORT`_ for more information.
   This generally only is useful under Einhorn where multiple processes are run
   on the same host.

.. _`this explanation of SO_REUSEPORT`: https://lwn.net/Articles/542629/

.. _server:

Loading the server
------------------

Baseplate.py now loads the actual server code that will run the main
application loop from here on out.

This process is very similar to loading your application code. The ``factory``
setting in the selected ``[server:foo]`` section of the configuration file is
inspected to determine which code to load. This is generally one of the server
implementations in Baseplate.py but you can write your own in your application
if needed. Once loaded, the rest of the configuration is passed onto the loaded
callable.

The new server object has expectations of what kind of application object your
application factory returned. For example, an HTTP server expects a :pep:`WSGI
<3333>` callable while the Thrift server expects a
:py:class:`~thrift.Thrift.TProcessor` object.

Handing off
-----------

Once everything is set up, Baseplate.py writes "Listening on <address>" to the
log and hands off control to the server object which is expected to serve
forever (unless one of the signals registered above is received) and use your
application to handle requests.
