# Matches the verbs used across the Convalesce Python repos.
.PHONY: lint test

lint:
	isort --settings-path . src plugins
	black --config pyproject.toml src plugins
	flake8 --config .flake8 src plugins
	mypy --config-file mypy.ini src plugins
	pylint --rcfile .pylintrc src/convalesce_emit plugins/*/src/convalesce_emit_*

test:
	pytest
