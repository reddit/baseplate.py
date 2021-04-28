A tiny "Hello, World" service
=============================

In this tutorial, we're going to build up a simple service to show off various
aspects of Baseplate.py.

Prerequisites
-------------

This tutorial expects you to be familiar with Python and the basics of web
application development.  We will use `Python 3.7`_ and `virtual
environments`_. To get set up, see `this guide on installing Python`_.

.. _`Python 3.7`: https://www.python.org/
.. _`virtual environments`: https://virtualenv.pypa.io/en/stable/
.. _`this guide on installing python`: https://realpython.com/installing-python/

Make a home for our service
---------------------------

First, let's create a folder and virtual environment to isolate the code and
dependencies for this project.

.. code-block:: console

   $ mkdir tutorial
   $ cd tutorial
   $ virtualenv --python=python3.7 venv
   Running virtualenv with interpreter /usr/bin/python3.7
   Using base prefix '/usr'
   New python executable in /home/user/tutorial/venv/bin/python3.7
   Also creating executable in /home/user/tutorial/venv/bin/python
   Installing setuptools, pkg_resources, pip, wheel...done.
   $ source venv/bin/activate

Build a simple Pyramid service
------------------------------

Pyramid is a mature web framework for Python that we build HTTP services with.
We'll start our service out by using it without Baseplate.py at all:

.. code-block:: console

   $ pip install pyramid

Now let's write a tiny Pyramid service, open your editor and put the following
in ``helloworld.py``:

.. literalinclude:: helloworld.py
   :language: python

Then run it:

.. code-block:: console

   $ python helloworld.py

Now that you have got a server running, let's try talking to it. From another
terminal:

.. code-block:: console

   $ curl localhost:9090
   {"Hello": "World"}

and the server should have logged about that request:

.. code-block:: none

   127.0.0.1 - - [06/Aug/2019 23:32:40] "GET / HTTP/1.1" 200 18

Great! It does not do much, but we have got a very basic service up and running
now.

Breaking it down
----------------

.. seealso::

   You can get way more detail about what's going on in Pyramid in :ref:`Pyramid's
   own tutorial <quick_tutorial>`.

There are three things going on in this tiny service. Following how the code
actually runs, we start out at the end of the file with the creation of the
HTTP server:

.. literalinclude:: helloworld.py
   :language: python
   :start-at: __main__

This is using the :py:mod:`wsgiref` module from the Python standard library to
run a basic development server. WSGI is the Python standard interface between
HTTP servers and applications. Pyramid applications are WSGI applications and
can be run on any WSGI server.

.. note::

   This server will do fine for this quick start, but we won't want to stick
   with it as we scale up as it can't handle multiple requests at the same
   time.

This server code calls our ``make_wsgi_app`` function to get the actual
application. Let's look at that next:

.. literalinclude:: helloworld.py
   :language: python
   :pyobject: make_wsgi_app

The real workhorse here is the :py:class:`~pyramid.config.Configurator` object
from Pyramid. This object helps us configure and build an application.

.. literalinclude:: helloworld.py
   :language: python
   :start-at: add_route
   :end-at: add_route

First off, we add a route that maps the URL path ``/`` to the route named
``hello_world`` when the HTTP verb is ``GET``. This means that when a request
comes in that matches those criteria, Pyramid will try to find a "view"
function that is registered for that route name.

.. literalinclude:: helloworld.py
   :language: python
   :start-at: scan
   :end-at: scan

Then we tell Pyramid to scan the current module for :ref:`declarative
registrations <decorations_and_code_scanning>`. Because of the ``@view_config``
decorator, Pyramid will find the ``hello_world`` function in our service and
recognize that we have registered it to handle the ``hello_world`` route.

.. literalinclude:: helloworld.py
   :language: python
   :pyobject: make_wsgi_app
   :start-at: return

Finally, we ask the configurator to build a WSGI application based on what we
have configured and return that to the server.

At this point, we have done the one-time application startup and handed off our
application to the server which is ready to call into it when requests come in.
Now it's time to look at the code that actually runs on each request.

.. literalinclude:: helloworld.py
   :language: python
   :pyobject: hello_world

This function gets called each time a matching request comes in. Pyramid will
build a :py:class:`~pyramid.request.Request` object and pass it into our
function as ``request``. This contains all the extra information about the
request, like form fields and header values. Whatever gets returned from this
function will be rendered by the ``renderer`` we specified in the
``@view_config`` and then sent to the client.

Summary
-------

We have built a tiny service on Pyramid and understand how the code all fits
together. So far, there's been no Baseplate.py at all. Next up, we'll look at
what's involved with adding it in.
