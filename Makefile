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
	touch $@

$(THRIFT_BUILDDIR)/tests/integration/test.thrift_buildstamp: tests/integration/test.thrift
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp -r $(THRIFT_BUILDDIR)/$</test tests/integration/test_thrift
	rm -f tests/integration/test_thrift/TestService-remote
	touch $@

docs:
	sphinx-build -M html docs/ build/

test:
	tox

lint:
	flake8
	pylint baseplate/
	mypy baseplate/

checks: tests lint spelling

clean:
	-rm -rf build/
	-rm -rf tests/integration/test_thrift/

realclean: clean
	-rm -rf baseplate.egg-info/

.PHONY: docs spelling clean realclean tests lint checks
