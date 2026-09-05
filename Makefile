# Delegates to each language's own gate.
.PHONY: lint test lint-python test-python lint-java test-java

lint: lint-python lint-java
test: test-python test-java

lint-python:
	$(MAKE) -C python lint

test-python:
	$(MAKE) -C python test

lint-java:
	cd java && ./gradlew --quiet spotlessCheck

test-java:
	cd java && ./gradlew --quiet test
