Baseplate imposes some additional semantics on code defined in .thrift files.

The namespace of the code defaults to (basename)_thrift instead of just the
base name of the file. Note that this default only applies if there is no
namespace declaration in the file. If one is present it will be respected, but
forced to be relative to the directory the thrift file exists in, rather than
being set to the root of the project.

So a file named ``foo/bar/baz.thrift``` is accessed as a python module named
``foo.bar.baz_thrift`` rather than the thrift standard of ``baz``.
If the baz.thrift file already has a ``namespace py quux`` declaration. It will be
accessed in python as ``foo.bar.quux``, not the thrift default of ``quux``.

Install this behavior by including
``baseplate.integration.command.ThriftBuildPyCommand`` as the ``build_py`` command in
your ``setup.py``.
