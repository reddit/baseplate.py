Linters
=======

Incorporating linters into your service will enforce a coding standard and prevent errors from getting merged into your codebase.
The :py:mod:`baseplate.lint` module consists of custom `Pylint`_ checkers based on bugs found at Reddit.

.. _`Pylint`: https://pylint.readthedocs.io/en/latest/intro.html

Configuration
-------------

Getting Started
^^^^^^^^^^^^^^^

`Install Pylint`_ and ensure you have it and its dependencies added to your requirements-dev.txt file.

.. _`Install Pylint`: https://pylint.readthedocs.io/en/latest/user_guide/installation.html

Follow the `Pylint user guide`_ for instructions to generate a default pylintrc configuration file and run Pylint.

.. _`Pylint user guide`: https://pylint.readthedocs.io/en/latest/user_guide/run.html

Adding Custom Checkers
^^^^^^^^^^^^^^^^^^^^^^

In your pylintrc file, add baseplate.lint to the [MASTER] load-plugins configuration.

.. code-block:: none

    # List of plugins (as comma separated values of python modules names) to load,
    # usually to register additional checkers.
    load-plugins=baseplate.lint

This will allow you to use all the custom checkers in the baseplate.lint module when you run Pylint.

Custom Checkers List
^^^^^^^^^^^^^^^^^^^^

* no-database-query-string-format
