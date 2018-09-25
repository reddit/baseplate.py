from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import glob
import os
import subprocess

from distutils.core import Command
from distutils.command.build_py import build_py

import baseplate.thrift


class BuildThriftCommand(Command):
    description = "Generate Python code from Thrift IDL."
    user_options = [
        ("build-base=", "b", "base directory for build library"),
    ]

    def initialize_options(self):
        self.build_base = None

    def finalize_options(self):
        self.set_undefined_options("build",
            ("build_base", "build_base"),
        )

    def run(self):
        if self.dry_run:
            return

        temp_dir = os.path.join(self.build_base, "thrift")
        self.mkpath(temp_dir)

        for package in self.distribution.packages:
            package_dir = os.path.join(*package.split("."))

            for thriftfile in glob.glob(os.path.join(package_dir, "*.thrift")):
                subprocess.check_call([
                    "thrift",
                    "-strict",
                    "-gen", "py:slots",
                    "-out", temp_dir,
                    "-I", os.path.dirname(baseplate.thrift.__file__),
                    thriftfile,
                ])

                module_name = os.path.splitext(os.path.basename(thriftfile))[0]
                input_package = os.path.join(temp_dir, module_name)

                generated_module = "_".join((module_name, "thrift"))
                output_package = os.path.join(package_dir, generated_module)

                self.copy_tree(input_package, output_package)

                # rewrite "from thriftname" to "from package.thriftname_thrift"
                full_package_name = "%s.%s" % (package, generated_module)
                for remote in glob.glob(os.path.join(output_package, "*-remote")):
                    with open(remote) as f:
                        lines = f.readlines()

                    with open(remote, "w") as f:
                        for line in lines:
                            prefix = "from " + module_name
                            if line.startswith(prefix):
                                f.write("from " + full_package_name + line[len(prefix):])
                            else:
                                f.write(line)


class ThriftBuildPyCommand(build_py):
    def run(self):
        self.run_command("build_thrift")
        build_py.run(self)
