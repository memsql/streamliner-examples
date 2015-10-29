SHELL := /bin/bash

VERSION := $(shell sbt 'export version' | tail -n 1)
export VERSION

.PHONY: default
default: build

.PHONY: version
version:
	@echo $(VERSION)

.PHONY: clean
clean:
	sbt clean \
		"project thrift" clean \
		"project avro" clean

##############################
# PROJECT BUILD RULES
#
.PHONY: build
build: clean
	sbt assembly

.PHONY: build-thrift
build-thrift: clean
	sbt "project thrift" assembly

.PHONY: build-avro
build-avro: clean
	sbt "project avro" assembly

##############################
# PROJECT TEST RULES
#
.PHONY: test
test:
	sbt test

.PHONY: thrift-test-deps-compile
thrift-test-deps-compile:
	mkdir -p thrift/src/test/java
	thrift -o thrift/src/test/java/ --gen java thrift/src/test/thrift/TestClass.thrift

.PHONY: test-thrift
test-thrift: thrift-test-deps-compile
	sbt "project thrift" test

.PHONY: test-avro
test-avro:
	sbt "project avro" test
