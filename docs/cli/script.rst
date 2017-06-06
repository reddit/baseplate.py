``baseplate-script``
====================

This command allows you to run a piece of Python code with the application
config loaded similarly to `baseplate-serve`_. The command is
``baseplate-script2`` or ``baseplate-script3`` depending on which version of
Python you would like to use.

.. _baseplate-serve: serve.html

Command Line
------------

There are two required arguments on the command line: the path to an INI-format
configuration file, and the fully qualified name of a Python function to run.

The function should be specified as a module path, a colon, and a function
name. For example, ``my_service.models:create_schema``. The function should
take a single argument which will be the application's configuration as a
dictionary. This is the same as the application factory used by the server.

Just like with ``baseplate-serve``, the ``app:main`` section will be loaded by
default. This can be overridden with the ``--app-name`` option.

Example
-------

Given a configuration file, ``printer.ini``:

.. code-block:: ini

   [app:main]
   message = Hello!

   [app:bizarro]
   message = !olleH

and a small script, ``printer.py``:

.. code-block:: python

   def run(app_config):
       print(app_config["message"])

You can run the script with various configurations:

.. code-block:: text

   $ baseplate-script2 printer.ini printer:run
   Hello!

   $ baseplate-script2 printer.ini --app-name=bizarro printer:run
   !olleH
