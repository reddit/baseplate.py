.PHONY: all
all: build

THRIFT=thrift1
THRIFT_OPTS=-strict -gen py:utf8strings,slots,new_style
THRIFT_BUILDDIR=build/thrift
THRIFT_SOURCE=baseplate/thrift/baseplate.thrift
THRIFT_BUILDSTAMPS=$(patsubst %,$(THRIFT_BUILDDIR)/%_buildstamp,$(THRIFT_SOURCE))

PYTHON=python

thrift: $(THRIFT_BUILDSTAMPS)

# we use a python namespace which causes a whole bunch of extra nested
# directories that we want to get rid of
$(THRIFT_BUILDDIR)/baseplate/thrift/baseplate.thrift_buildstamp: baseplate/thrift/baseplate.thrift
	@echo SPECIAL $< $@
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp -r $(THRIFT_BUILDDIR)/$</baseplate/thrift/ baseplate/
	touch $@

.PHONY: build
build: thrift
	$(PYTHON) setup.py build

.PHONY: docs
docs:
	$(PYTHON) setup.py build_sphinx -b html

.PHONY: clean
clean:
	-rm -rf build/

.PHONY: realclean
realclean: clean
	-rm -rf baseplate.egg-info/

.PHONY: nosetests
nosetests: build
	$(PYTHON) setup.py nosetests

.PHONY: doctests
doctests: build
	$(PYTHON) setup.py --help
	$(PYTHON) setup.py --help-commands
	$(PYTHON) setup.py build_sphinx -b doctest

.PHONY: spelling
spelling:
	$(PYTHON) setup.py build_sphinx -b spelling

.PHONY: tests
tests: nosetests doctests spelling

.PHONY: alltests
alltests:
	make PYTHON=python2.7 tests
	make PYTHON=python3.4 tests
