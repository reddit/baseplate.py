Quick Start
===========

Installation
------------

To install Baseplate, use the debian packages::

   sudo add-apt-repository ppa:reddit/ppa
   sudo apt-get update
   sudo apt-get install python-gevent python-baseplate

Cookiecutter
------------

Baseplate has a project generation tool to help you get started.
Install it::

   pip install git+https://github.com/reddit/baseplate-cookiecutter

Then run the tool and follow the prompts::

   baseplate-cookiecutter

The templates have liberal "TODO"s for you to fill in your application. Good
luck!

Serving
-------

Once your service is ready (or right now if you are impatient)  you can start
serving requests::

   baseplate-serve2 example.ini

For more info on this mysterious ``baseplate-serve2``, check out the `chapter
about the baseplate server`_.

.. _chapter about the baseplate server: cli/serve.html
