import sys

from setuptools import setup, find_packages


PY3 = (sys.version_info.major == 3)

install_requires = [
    "requests",
    "posix_ipc",
],

tests_require = [
    "nose",
    "coverage",
    "webtest",
]

if not PY3:
    tests_require.append("mock")

extras_require = {
    "gevent": [
        "gevent",
    ],

    "thrift": [
        "thrift",
    ],

    "pyramid": [
        "pyramid",
    ],
}


setup(
    name="baseplate",
    version="0.4.0",
    packages=find_packages(exclude=["tests"]),

    install_requires=install_requires,
    extras_require=extras_require,

    test_suite="tests",
    tests_require=tests_require,

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
    },
)
