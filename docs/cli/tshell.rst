``baseplate-tshell``
====================

This command allows you to run an interactive Python shell for a Thrift service
with the application config and context loaded. The command is
``baseplate-tshell``.

HTTP services can use Pyramid's pshell_ in order to get an interactive shell.

.. _pshell: https://docs.pylonsproject.org/projects/pyramid/en/latest/pscripts/pshell.html

Command Line
------------

This command requires the path to an INI-format configuration file to run.

Just like with ``baseplate-serve``, the ``app:main`` section will be loaded by
default. This can be overridden with the ``--app-name`` option.

By default, the shell will have variables containing the application and the
context exposed. Additional variables can be exposed by providing a ``setup``
function in the ``tshell`` section of the configuration file.

Example
-------

Given a configuration file, ``example.ini``:

.. code-block:: ini

   [app:main]
   factory = baseplate.server.thrift

   [tshell]
   setup = my_service:tshell_setup

and a small setup function, ``my_service.py``:

.. code-block:: python

   def tshell_setup(env, env_banner):
       from my_service import models
       env['models'] = models
       env_banner['models'] = 'Models module'

You can begin a shell with the models module exposed:

.. code-block:: text

   $ tshell example.ini
   Baseplate Interactive Shell
   Python 2.7.6 (default, Nov 23 2017, 15:49:48)
   [GCC 4.8.4]

   Available Objects:

     app          This project's app instance
     context      The context for this shell instance's span
     models       Models module
   >>>
