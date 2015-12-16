Glossary
========

.. glossary::

   Context Object
      An object containing per-request state passed into your request handler. The exact form it
      takes depends on the framework you are using.

      Thrift
         The ``context`` object passed into handler functions when using a
         ``ContextProcessor``.

      Pyramid
         The ``request`` object passed into views.
