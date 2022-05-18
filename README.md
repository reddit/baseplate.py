# baseplate.py

* [Documentation](https://baseplate.readthedocs.io/en/stable/)
* [Contribution Guidelines](https://github.com/reddit/baseplate.py/blob/develop/CONTRIBUTING.md)

It's much easier to manage a bunch of services when they all have the same
shape: the way they're developed, the way they interact with the infrastructure
they run on, and the way they interact with each other. Baseplate is reddit's
specification for the common shape of our services. This library, Baseplate.py,
is the Python implementation of that specification.

Baseplate.py glues together tooling for interacting with the reddit backend
ecosystem and spackles over things that are missing. It integrates with Apache
Thrift, Pyramid, and client libraries for many systems to transparently make
your applications observable.

Baseplate applications transparently get:

* Timing and request rate metrics using statsd
* Distributed tracing with Zipkin
* Error reporting and aggregation with Sentry

And can take advantage of:

* Integration with commonly used clients like: Thrift, SQLAlchemy,
  cassandra-driver, pymemcache, redis-py, and Kombu
* Secrets securely pulled from Vault

And many other things!

# pre-commit

* [Documentation](https://pre-commit.com/)

This repo comes with pre-commit hooks that let you (on a voluntary basis)
enable pre-commit and/or pre-push hooks.

Configuration can be found at the root of the directory in
`.pre-commit-config.yaml`. On its own, the configuration file doesn't do
anything. You either need to [run the hooks manually](https://pre-commit.com/#pre-commit-run)
or [install them](https://pre-commit.com/#pre-commit-install) so that they run
automatically on every commit or push.

Currently, we run the `make fmt` target on commit and `make lint` / `pytest`
actions on push.

Specific hooks can be [temporarily disabled](https://pre-commit.com/#temporarily-disabling-hooks).

You can install hooks only for a specific step (i.e. [pre-push](https://pre-commit.com/#pre-commit-during-push)).
