import itertools

from setuptools import find_packages
from setuptools import setup


extras_require = {
    "amqp": ["kombu>=4.0.0,<5.0"],
    "cassandra": ["cassandra-driver>=3.13.0,<4.0"],
    "cqlmapper": ["reddit-cqlmapper>=0.3.0,<1.0"],
    "kafka": ["confluent-kafka>=1.3,<2.0"],
    "memcache": ["pymemcache>=1.3.0,<1.4.4"],
    "prometheus": ["prometheus-client>=0.12.0,<1.0"],
    "pyramid": ["pyramid>=1.9.0,<2.0"],
    "redis": ["redis>=2.10.0,<4.0.0"],
    "redis-py-cluster": ["redis-py-cluster>=2.1.2,<3.0.0"],
    "refcycle": ["objgraph>=3.0,<4.0"],
    "requests": ["advocate>=1.0.0,<2.0"],
    "sentry": ["sentry-sdk>=0.19,<1.0"],
    "sql": ["sqlalchemy>=1.1.0,<2.0"],
    "zookeeper": ["kazoo>=2.5.0,<3.0"],
}
extras_require["all"] = list(itertools.chain.from_iterable(extras_require.values()))

setup(
    name="baseplate",
    description="reddit's python service framework",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="reddit",
    license="BSD",
    url="https://github.com/reddit/baseplate.py",
    project_urls={
        "Documentation": "https://baseplate.readthedocs.io/en/stable/",
    },
    use_scm_version=True,
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.7",
    setup_requires=["setuptools_scm"],
    install_requires=[
        "posix_ipc>=1.0.0,<2.0",
        "python-json-logger>=2.0,<3.0",
        "requests>=2.21.0,<3.0",
        "thrift-unofficial>=0.14.1,<1.0",
        "gevent>=20.5.0",
        "prometheus-client>=0.12.0",
    ],
    extras_require=extras_require,
    scripts=[
        "bin/baseplate-script",
        "bin/baseplate-serve",
        "bin/baseplate-shell",
        "bin/baseplate-tshell",
        "bin/baseplate-healthcheck",
    ],
    # the thrift compiler must be able to find baseplate.thrift to build
    # services which extend BaseplateService.
    package_data={"baseplate": ["py.typed"], "baseplate.thrift": ["*.thrift"]},
    zip_safe=False,
    entry_points={
        "distutils.commands": [
            "build_thrift = baseplate.frameworks.thrift.command:BuildThriftCommand"
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)
