# Delegates to each language's own gate.
.PHONY: lint test lint-python test-python lint-java test-java e2e

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

# One tool, one version, for real in Docker. See e2e/README.md.
e2e:
	cd e2e && EMIT_E2E_$$(echo $(TOOL) | tr a-z A-Z)=$(VERSION) python -m pytest test_$(TOOL).py
