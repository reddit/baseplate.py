# Contribution Guidelines

## Testing and Linting

Our CI system will run a ton of checks on pull requests. You can run these
checks locally before submitting changes as described below.

A `Makefile` full of actions and a [Docker Compose] development environment are
provided. [Install Docker Compose] if you don't already have it, then build the
base image:

```console
$ docker-compose build
```

Now you can use `docker-compose run` various commands to format, lint, test,
and compile the library. For example:

```console
$ docker-compose run baseplate make test
```

Here are some of the Make targets available for use:

* `make fmt`: Automatically reformat code to match the style guide.
* `make lint`: Run the full suite of linters against the codebase. A report of
  type hint coverage is written to `build/mypy/`.
* `make test`: Run the test suite. A report of test coverage is written to
  `build/coverage/`.
* `make docs`: Build the HTML documentation. The output will be in `build/html/`.
* `make thrift`: Compile the Thrift IDL into Python code. Run this any time you
  change `baseplate/thrift/baseplate.thrift` or
  `tests/integration/test.thrift`. You'll generally need to follow this up with
  `make fmt`.
* `make clean`: Delete intermediate build files.

You can also run arbitrary commands, e.g. to run a single portion of the test
suite:

```console
$ docker-compose run baseplate pytest -v tests/integration/pyramid_tests.py
```

Don't forget to `docker-compose down` when you're done to free up resources.

[Docker Compose]: https://docs.docker.com/compose/
[Install Docker Compose]: https://docs.docker.com/compose/install/
