# Matches the verbs used across the Convalesce Python repos.
.PHONY: lint test

lint:
	isort src plugins
	black src plugins
	flake8 src plugins
	mypy src plugins
	pylint src/convalesce_emit plugins/*/src/convalesce_emit_*

test:
	pytest
