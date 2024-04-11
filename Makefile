REORDER_PYTHON_IMPORTS := reorder-python-imports --py3-plus --separate-from-import --separate-relative
PYTHON_SOURCE = $(shell find baseplate/ tests/ -name '*.py')
PYTHON_EXAMPLES = $(shell find docs/ -name '*.py')

all: thrift

THRIFT=thrift
THRIFT_OPTS=-strict -gen py:slots
THRIFT_BUILDDIR=build/thrift
THRIFT_SOURCE=baseplate/thrift/baseplate.thrift tests/integration/test.thrift
THRIFT_BUILDSTAMPS=$(patsubst %,$(THRIFT_BUILDDIR)/%_buildstamp,$(THRIFT_SOURCE))

thrift: $(THRIFT_BUILDSTAMPS)

# we use a python namespace which causes a whole bunch of extra nested
# directories that we want to get rid of
$(THRIFT_BUILDDIR)/baseplate/thrift/baseplate.thrift_buildstamp: baseplate/thrift/baseplate.thrift
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp -r $(THRIFT_BUILDDIR)/$</baseplate/thrift baseplate/
	rm -f baseplate/thrift/BaseplateService-remote
	rm -f baseplate/thrift/BaseplateServiceV2-remote
	touch $@

$(THRIFT_BUILDDIR)/tests/integration/test.thrift_buildstamp: tests/integration/test.thrift
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp $(THRIFT_BUILDDIR)/$</test/* tests/integration/test_thrift
	rm -f tests/integration/test_thrift/TestService-remote
	touch $@

.PHONY: docs
docs:
	poetry run sphinx-build -M html docs/ build/

.PHONY: doctest
doctest:
	poetry run sphinx-build -M doctest docs/ build/

.PHONY: linkcheck
linkcheck:
	poetry run sphinx-build -M linkcheck docs/ build/

.PHONY: test
test: doctest
	# Some files use gevent to monkey patch stdlib functions. This causes problems
	# if it happens after importing the sequential versions of some of these. Thus
	# we need to do it as early as possible.
	poetry run python -m gevent.monkey --module pytest -v tests/

.PHONY: fmt
fmt:
	poetry run $(REORDER_PYTHON_IMPORTS) --exit-zero-even-if-changed $(PYTHON_SOURCE)
	poetry run black baseplate/ tests/
	poetry run $(REORDER_PYTHON_IMPORTS) --application-directories /tmp --exit-zero-even-if-changed $(PYTHON_EXAMPLES)
	poetry run black docs/  # separate so it uses its own pyproject.toml

.PHONY: lint
lint:
	poetry run $(REORDER_PYTHON_IMPORTS) --diff-only $(PYTHON_SOURCE)
	poetry run black --diff --check baseplate/ tests/
	poetry run flake8 baseplate tests
	PYTHONPATH=. poetry run pylint baseplate/
	poetry run mypy baseplate/

.PHONY: checks
checks: test lint linkcheck

.PHONY: clean
clean:
	-rm -rf build/
	-rm -rf tests/integration/test_thrift/

.PHONY: realclean
realclean: clean
	-rm -rf baseplate.egg-info/
