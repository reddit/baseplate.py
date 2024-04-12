# Contributing to Baseplate

Our CI system will run a ton of checks on pull requests. You can run these
checks locally before submitting changes as described below.

## Setting up your development environment

You'll need the following available to work on Baseplate:

* **Docker and [Docker Compose]:** These are used for running tests in a
  consistent environment. You can [install Docker
  Compose][docker-compose-install] if you don't already have it.

* **Poetry:** This is used for managing the Python project and dependencies.
  You can [follow the official instructions][poetry-install] or use `brew
  install poetry` on macOS.

Once you have these tools installed, run `make venv` to have Poetry create a
virtualenv inside a `.venv` directory. You should run this command any time you
pull in dependency changes so that Poetry can update your virtualenv.


## Running linters

You can use these Make targets:

* `make fmt`: Automatically reformat code to match the style guide.
* `make lint`: Run the full suite of linters against the codebase. A report of
  type hint coverage is written to `build/mypy/`.


## Running tests

We recommend running tests using Docker Compose. This helps to have testing
services running, and allows tests to run on macOS (which is normally not
possible due to the use of `posix_ipc.MessageQueue`).

First, build an image containing the Baseplate code:

```console
$ docker-compose build
```

Now you can use `docker-compose run` various commands to test the library. For
example:

```console
$ docker-compose run baseplate make test
```

Here are some of the Make targets available for use:

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


## Managing dependencies

You can use all of the [standard Poetry commands][poetry-commands] to manage
dependencies.


[Docker Compose]: https://docs.docker.com/compose/
[docker-compose-install]: https://docs.docker.com/compose/install/
[poetry-install]: https://python-poetry.org/docs/#installation
[poetry-commands]: https://python-poetry.org/docs/cli/
