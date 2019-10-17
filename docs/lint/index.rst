Linters
=======

Incorporating linters into your service will enforce a coding standard and prevent errors from getting merged into your codebase.
The :py:mod:`baseplate.lint` module consists of custom `Pylint`_ checkers which add more lint to Pylint. These lints are based on bugs found at Reddit.

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

In your ``pylintrc`` file, add ``baseplate.lint`` to the ``[MASTER]`` load-plugins configuration.

.. code-block:: none

    # List of plugins (as comma separated values of python modules names) to load,
    # usually to register additional checkers.
    load-plugins=baseplate.lint

This will allow you to use all the custom checkers in the baseplate.lint module when you run Pylint.

.. _custom-checkers-list:

Custom Checkers List
^^^^^^^^^^^^^^^^^^^^

* W9000: no-database-query-string-format

Creating Custom Checkers
^^^^^^^^^^^^^^^^^^^^^^^^

If there is something you want to lint and a checker does not already exist, you can add a new one to :py:mod:`baseplate.lint`.

The following is an example checker you can reference to create your own.

.. literalinclude:: ../../baseplate/lint/example_plugin.py
    :language: python

Add a test to the baseplate test suite following this example checker test.

.. literalinclude:: ../../tests/unit/lint/example_plugin_tests.py
    :language: python

Register your checker by adding it to the register() function:

.. literalinclude:: ../../baseplate/lint/__init__.py
    :language: python

Lastly, add your checker message-id and name to :ref:`custom-checkers-list`.
