all: thrift build

THRIFT=thrift1
THRIFT_OPTS=-strict -gen py:utf8strings,slots,new_style
THRIFT_BUILDDIR=build/thrift
THRIFT_SOURCE=baseplate/thrift/baseplate.thrift
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

build:
	python2 setup.py build
	python3 setup.py build

docs:
	sphinx-build -M html docs/ build/

spelling:
	sphinx-build -M spelling docs/ build/

clean:
	-rm -rf build/

realclean: clean
	-rm -rf baseplate.egg-info/

tests:
	nosetests
	nosetests3
	sphinx-build -M doctest docs/ build/

develop:
	python2 setup.py develop
	python3 setup.py develop

install:
	python2 setup.py install
	python3 setup.py install


.PHONY: docs spelling clean realclean tests develop install build
