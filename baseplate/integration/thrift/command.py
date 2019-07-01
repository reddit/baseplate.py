import glob
import os
import subprocess

from distutils.core import Command
from distutils.command.build_py import build_py

import baseplate.thrift


class BuildThriftCommand(Command):
    """
    Baseplate imposes some additional semantics on code defined in .thrift files.

    The namespace of the code defaults to (basename)_thrift instead of just the
    base name of the file.  The effective name space is also relative to the
    package the .thrift file is in rather than the root of the project.

    So a foo/bar/baz.thrift is accessed as a python module named
    `foo.bar.baz_thrift` rather than the thrift standard of `baz`.
    """

    description = "Generate Python code from Thrift IDL."
    user_options = [("build-base=", "b", "base directory for build library")]

    def initialize_options(self):
        self.build_base = None

    def finalize_options(self):
        self.set_undefined_options("build", ("build_base", "build_base"))

    def compile_thrift_file(self, temp_dir, thrift_file):
        subprocess.check_call(
            [
                "thrift",
                "-strict",
                "-gen",
                "py:slots",
                "-out",
                temp_dir,
                "-I",
                os.path.dirname(baseplate.thrift.__file__),
                thrift_file,
            ]
        )

    def namespace_declaration(self, line):
        if line.startswith("namespace py") or line.startswith("namespace *"):
            words = line.split()
            if len(words) > 2:
                return words[2]
        return False

    def copy_with_namespace(self, thrift_file, buildfile):
        """
        Patch or create the namespace declaration to include the path from the
        source root to this file.  If it didn't already exist, the base name
        will be set to (module name)_thrift, not the thrift default of (module
        name).

        Returns the python module name of the namespace.
        """
        with open(thrift_file, "rt") as fd, open(buildfile, "wt") as out:
            # First, find any pre-existing namespace declaration
            namespace = None
            for line in fd:
                ns = self.namespace_declaration(line)
                if ns:
                    namespace = ns
                    break
            if not namespace:
                # No namespace declaration in the source. Set one up by
                # using the module name plus _thrift to avoid conflicts
                # with non thrift modules.
                namespace = os.path.splitext(os.path.basename(thrift_file))[0]
                namespace += "_thrift"

            # Patch the namespace to reflect the full path from the source
            # root to this file so the generated imports work.
            namespace = os.path.dirname(thrift_file).replace("/", ".") + "." + namespace

            fd.seek(0, 0)
            for line in fd:
                # Strip out any pre-existing namespace declarations so that our
                # modified declaration is honored.
                if self.namespace_declaration(line):
                    out.write(f"namespace py {namespace}\n")
                else:
                    out.write(line)
            out.flush()
        return namespace

    def build(self, thrift_file, build_dir, package_dir, python_namespace):
        print(f"building {thrift_file} -> {python_namespace}")
        # named 'module', unless there is a namespace declaration.
        filename = os.path.basename(thrift_file)
        package = python_namespace.replace(".", "/")
        built_package = os.path.join(build_dir, package)

        # move the built 'module' directory to a directory 'module_thrift'
        # at the same path as the original 'module.thrift'. The rename to
        # module_thrift is handled by copy_with_namespace because handling
        # references to this code from other thrift files requires adjusting
        # namespace declarations.
        # output_package is interpreted relative to the input source, which is
        # the current working directory.
        output_package = package

        self.copy_tree(built_package, output_package)

    def find_thrift_files(self):
        """
        Returns: [(thrift file path, containing package root)]
        """
        thrift_packages = []
        for package in self.distribution.packages:
            package_dir = os.path.join(*package.split("."))
            for thrift_file in glob.glob(os.path.join(package_dir, "*.thrift")):
                thrift_packages.append((thrift_file, package_dir))
        return thrift_packages

    def run(self):
        if self.dry_run:
            return

        temp_dir = os.path.join(self.build_base, "thrift")
        self.mkpath(temp_dir)

        # Find all of the thrift files in this project.
        thrift_packages = self.find_thrift_files()

        # Copy them all to the clean temp dir, and patch their namespace
        # declarations to match where we found them in the source.
        buildfiles = []
        for thrift_file, package_dir in thrift_packages:
            build_file = os.path.join(temp_dir, thrift_file)
            build_package = os.path.dirname(build_file)
            if not os.path.exists(build_package):
                os.makedirs(build_package)
            py_namespace = self.copy_with_namespace(thrift_file, build_file)
            buildfiles.append((thrift_file, build_file, package_dir, py_namespace))

        # Then compile them all.
        for thrift_file, build_file, package_dir, py_namespace in buildfiles:
            self.build(build_file, temp_dir, package_dir, py_namespace)


class ThriftBuildPyCommand(build_py):
    def run(self):
        self.run_command("build_thrift")
        build_py.run(self)
