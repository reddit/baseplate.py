from setuptools import find_packages
from setuptools import setup

setup(
    name="baseplate",
    description="reddit's python service framework",
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
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
        "posix_ipc>=1.0.0",
        "python-json-logger~=0.1",
        "requests>=2.21.0",
        "thrift>=0.12.0",
        "gevent>=20.5.0",
    ],
    extras_require={
        "amqp": ["kombu>=4.0.0"],
        "cassandra": ["cassandra-driver>=3.13.0"],
        "cqlmapper": ["cqlmapper>=0.1.0"],
        "memcache": ["pymemcache>=1.3.0,<=2.0.0"],
        "pyramid": ["pyramid>=1.9.0"],
        "redis": ["redis>=2.10.0,<=4.0.0"],
        "requests": ["advocate>=1.0.0"],
        "sql": ["sqlalchemy>=1.1.0"],
        "zookeeper": ["kazoo>=2.5.0"],
    },
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
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)
