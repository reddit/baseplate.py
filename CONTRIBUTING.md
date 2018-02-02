# Contributing to Baseplate

Thank you for wanting to add something to Baseplate! This document has some
tips for how to work with it.

## Design

If you're building a whole new feature, it may make the most sense to try it
out in a running service first. This allows you to shake out any API design
problems or bugs before committing it to the library which generally has a
slower release cycle. It's also really helpful to write a design document for
new features.

## Vagrant

Baseplate comes with a Vagrantfile which sets up a whole development
environment. All the commands referenced in this document are expected to be
run in the root of the Baseplate source directory inside the created Vagrant.

Install [Vagrant](https://www.vagrantup.com/) then run the following inside the
Baseplate directory:

    vagrant up

This will take a while while it sets everything up, then you can:

    vagrant ssh

You'll now be inside the VM and can run things:

    cd baseplate/
    make checks

which will run the test, lint, and documentation steps fully to ensure
everything's good to go. Please make sure to always run this before submitting
pull requests.

## Testing

Baseplate has a test suite in `tests/`. It is divided into unit and integration
tests. The integration tests will often communicate with various servers (e.g.
Cassandra) that are set up in the Vagrant environment.

If you want to run just the test suite alone, you can run:

    make tests

which will run just the test suite for both Python 2 and Python 3. You can also
run `nosetests` directly to further narrow your test run to one language or
even a specific module:

    nosetests tests.unit.metrics_tests

Coverage reports are also automatically generated. While we don't strive for
100% coverage, it's good to see what's missing. You can see the detailed report
including color-coded lines at <http://baseplate.local/coverage/> after running
the test suite. (Note that if you run `make tests` the last test suite to run,
Python 3, will be the one shown).

## Documentation

Baseplate's documentation is generated with Sphinx. Configuration for Sphinx is
managed in the `docs/` subdirectory. When adding new modules or new top-level
items in modules, you'll generally need to create or update a `.rst` file in
`docs/`. Updating a docstring in the code doesn't usually require any changes
to `docs/`.

To see the generated HTML for the docs, run:

    make docs

and then visit <http://baseplate.local/html/> to see the results.

## Linting

Baseplate adheres to [reddit's
styleguide](https://github.com/reddit/styleguide). To check the code for
violations, run:

    make lint

The automated checks aren't yet fully comprehensive, so please do read the
styleguide for any other nuances. The goal is to favor readability and avoid
surprises.
