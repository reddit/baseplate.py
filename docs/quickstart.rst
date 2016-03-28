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

Baseplate has a few `Cookiecutter <https://cookiecutter.readthedocs.org>`_
templates available to make a skeleton for your new project.

For `Thrift`_::

   cookiecutter git@github.com:reddit/reddit-cookiecutter-thrift

For `Pyramid`_::

   cookiecutter git@github.com:reddit/reddit-cookiecutter-pyramid

The templates have liberal "TODO"s for you to fill in your application. Good
luck!

.. _Thrift: https://github.com/reddit/reddit-cookiecutter-thrift
.. _Pyramid: https://github.com/reddit/reddit-cookiecutter-pyramid

Serving
-------

Once your service is ready (or right now if you are impatient)  you can start
serving requests::

   baseplate-serve2 example.ini

For more info on this mysterious ``baseplate-serve2``, check out the `chapter
about the baseplate server`_.

.. _chapter about the baseplate server: server.html
