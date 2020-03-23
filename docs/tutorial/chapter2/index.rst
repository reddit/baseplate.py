Servers and Configuration files
===============================

In this chapter, we'll lay the foundation for using Baseplate.py in the
service.

Install Baseplate.py
--------------------

First off, let's install Baseplate.py in your virtual environment so we can
start using its components.

.. code-block:: console

   $ pip install 'git+https://github.com/apache/thrift#egg=thrift&subdirectory=lib/py'
   $ pip install git+https://github.com/reddit/baseplate.py

In the previous chapter, we made our service run its own HTTP/WSGI server. Now
we're going to use Baseplate.py's server instead which is run with
:program:`baseplate-serve`.

.. code-block:: console

   $ baseplate-serve
   usage: baseplate-serve [-h] [--debug] [--reload] [--app-name NAME]
                       [--server-name NAME] [--bind ENDPOINT]
                       config_file
   baseplate-serve: error: the following arguments are required: config_file

Uh oh! ``config_file``!? I guess we have got some more to do first.

A configuration file
--------------------

Baseplate services rely on configuration to allow them to behave differently in
different environments (development, staging, production, etc.). For
Baseplate.py, configuration is stored in a file in standard Python INI file
format as understood by :py:mod:`configparser`.

Open a new ``helloworld.ini`` in the tutorial directory and copy this into it:

.. literalinclude:: basic.ini
   :language: ini

Breaking it down, there are two sections to this configuration file,
``[app:main]`` and ``[server:main]``.

.. literalinclude:: basic.ini
   :language: ini
   :start-at: [app:main]
   :end-before: [server:main]

The first section defines the entrypoint and settings for the application
itself. The ``factory`` is a function that returns an application object. In
this case, it lives in the Python module ``helloworld`` and the function is
called ``make_wsgi_app``.

.. literalinclude:: basic.ini
   :language: ini
   :start-at: [server:main]

The second section defines what kind of server we'll run and the settings for
that server. Since our application is built for HTTP/WSGI, we use the WSGI
server in Baseplate.py.

.. note::

   You might notice that both application and server sections have ``:main`` in
   their names.  By default, Baseplate.py tools like :program:`baseplate-serve`
   will look for the sections with ``main`` in them, but you can override this
   with ``--app-name=foo`` to look up ``[app:foo]`` or ``--server-name``
   similarly.  This allows you to have multiple applications and servers
   defined in the same configuration file.

OK! Now let's try :program:`baseplate-serve` with our configuration file.

.. code-block:: console

   $ baseplate-serve helloworld.ini
   Traceback (most recent call last):
     File "/home/user/tutorial/venv/bin/baseplate-serve", line 14, in <module>
       load_app_and_run_server()
     File "/home/user/tutorial/venv/lib/python3.7/site-packages/baseplate/server/__init__.py", line 226, in load_app_and_run_server
       app = make_app(config.app)
     File "/home/user/tutorial/venv/lib/python3.7/site-packages/baseplate/server/__init__.py", line 180, in make_app
       return factory(app_config)
   TypeError: make_wsgi_app() takes 0 positional arguments but 1 was given

It looks like we'll need a little bit more.

Run the service with :program:`baseplate-serve`
-----------------------------------------------

In the previous section, we learned that the ``[app:main]`` section both tells
:program:`baseplate-serve` where to find the application *and* holds
configuration for that application. The function that we specify in ``factory``
needs to take a dictionary of the raw configuration values as an argument.
Let's add that to our service.

.. literalinclude:: serve_ready.py
   :language: python
   :emphasize-lines: 10-11

All we had to do was add one parameter. We also pass it through to Pyramid's
Configurator so any :ref:`framework-specific settings <environment_chapter>`
can be picked up.

Since we're not using the :py:mod:`wsgiref` server anymore, we can drop the
whole ``if __name__ == "__main__":`` section at the end of the file now.

Alright, third time's the charm, right?

.. code-block:: console

   $ baseplate-serve --debug helloworld.ini
   12593:MainThread:baseplate.server.runtime_monitor:INFO:No metrics client configured. Server metrics will not be sent.
   12593:MainThread:baseplate.server:INFO:Listening on ('127.0.0.1', 9090)

Success! The ``--debug`` flag will turn on some extra log messages, so we can
see a request log when we try hitting the service with :program:`curl` again.

.. code-block:: console

   $ curl localhost:9090
   {"Hello": "World"}

And something shows up in the server's logs:

.. code-block:: none

   12593:DummyThread-1:baseplate.server.wsgi:DEBUG:127.0.0.1 - - [2019-08-07 23:42:32] "GET / HTTP/1.1" 200 147 0.007743

You'll notice the logs look a bit different from before.
:program:`baseplate-serve` adds some extra info to help give context to your
log entries. That ``DummyThread-1`` is pretty useless though, so we'll make it
useful in the next chapter.

Summary
-------

We have now made a configuration file and made it possible to run our service
with :program:`baseplate-serve`.

So what did any of this do for us? :program:`baseplate-serve` is how we let
production infrastructure run our application and interact with it. It knows
how to process multiple requests simultaneously and will handle things like the
infrastructure asking it to gracefully shut down.

But the real fun of Baseplate.py comes when we start using its framework
integration to get some visibility into the guts of the application. Let's see
what that looks like in the next chapter.
