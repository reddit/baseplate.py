from setuptools import setup, find_packages

setup(
    name="baseplate",
    description="A library to build services on",
    long_description=open("README.rst").read(),
    author="Neil Williams",
    author_email="neil@reddit.com",
    license="BSD",
    url="https://baseplate.readthedocs.io/en/stable/",
    use_scm_version=True,

    packages=find_packages(exclude=["tests", "tests.*"]),

    setup_requires=[
        "setuptools_scm",
    ],

    install_requires=[
        "enum34; python_version <= '3.4'",
        "posix_ipc",
        "pyjwt",
        "requests",
    ],

    scripts=[
        "bin/baseplate-serve",
        "bin/baseplate-script",
        "bin/baseplate-tshell",
        "bin/baseplate-healthcheck",
    ],

    # the thrift compiler must be able to find baseplate.thrift to build
    # services which extend BaseplateService.
    package_data={
        "baseplate.thrift": [
            "*.thrift"
        ],
    },
    zip_safe=False,
    entry_points={
        "distutils.commands": [
            "build_thrift = baseplate.integration.thrift.command:BuildThriftCommand",
        ],

        "paste.app_factory": [
            "main = baseplate.integration.pyramid:paste_make_app",
        ],
    },

    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)
