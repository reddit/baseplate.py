all: thrift

THRIFT=thrift1
THRIFT_OPTS=-strict -gen py:utf8strings,slots,new_style
THRIFT_BUILDDIR=build/thrift
THRIFT_SOURCE=baseplate/thrift/baseplate.thrift tests/integration/test.thrift
THRIFT_BUILDSTAMPS=$(patsubst %,$(THRIFT_BUILDDIR)/%_buildstamp,$(THRIFT_SOURCE))

thrift: $(THRIFT_BUILDSTAMPS)

# we use a python namespace which causes a whole bunch of extra nested
# directories that we want to get rid of
$(THRIFT_BUILDDIR)/baseplate/thrift/baseplate.thrift_buildstamp: baseplate/thrift/baseplate.thrift
	@echo SPECIAL $< $@
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp -r $(THRIFT_BUILDDIR)/$</baseplate/thrift/ baseplate/
	touch $@

$(THRIFT_BUILDDIR)/tests/integration/test.thrift_buildstamp: tests/integration/test.thrift
	@echo SPECIAL $< $@
	mkdir -p $(THRIFT_BUILDDIR)/$<
	$(THRIFT) $(THRIFT_OPTS) -out $(THRIFT_BUILDDIR)/$< $<
	cp -r $(THRIFT_BUILDDIR)/$</test tests/integration/test_thrift
	touch $@

docs:
	sphinx-build -M html docs/ build/

spelling:
	sphinx-build -M spelling docs/ build/

tests: thrift
	nosetests -v
	nosetests3 -v
	sphinx-build -M doctest docs/ build/

lint:
	flake8 baseplate/
	pylint --errors-only baseplate/

checks: tests lint spelling

clean:
	-rm -rf build/
	-rm -rf tests/integration/test_thrift/

realclean: clean
	-rm -rf baseplate.egg-info/

.PHONY: docs spelling clean realclean tests lint checks
