SHELL := /bin/bash

VERSION := $(shell sbt 'export version' | tail -n 1)
export VERSION

default: build

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	sbt clean \
		"project thrift" clean

##############################
# PROJECT BUILD RULES
#
.PHONY: build
build: clean
	sbt assembly

.PHONY: build-thrift
build-thrift: clean
	sbt "project thrift" assembly

##############################
# PROJECT TEST RULES
#
.PHONY: thrift-test-deps-compile
thrift-test-deps-compile:
	mkdir -p thrift/src/test/java
	thrift -o thrift/src/test/java/ --gen java thrift/src/test/thrift/TestClass.thrift

.PHONY: test-thrift
test-thrift: thrift-test-deps-compile
	sbt "project thrift" test
