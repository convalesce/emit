# Matches the verbs used across the Convalesce Python repos.
#
# The stacks under ../e2e are linted here too, against this directory's
# configuration, named explicitly because the tools otherwise look for it
# beside the files.
.PHONY: lint test

E2E = ../e2e
E2E_PY = $(shell find $(E2E) -name '*.py')

lint:
	isort --settings-path . src plugins $(E2E)
	black --config pyproject.toml src plugins $(E2E)
	flake8 --config .flake8 src plugins $(E2E)
	mypy --config-file mypy.ini src plugins $(E2E)
	pylint --rcfile .pylintrc src/convalesce_emit plugins/*/src/convalesce_emit_* $(E2E_PY)

test:
	pytest
