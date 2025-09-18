VENV_PATH = .venv

.PHONY: setup-git
setup-git:
	pip install pre-commit==2.13.0
	pre-commit install --install-hooks

.PHONY: all
all: venv

.PHONY: venv-python
venv-python:
	virtualenv -ppython3 $(VENV_PATH)
	cd py && pip install -e .


.PHONY: test-python
test-python: venv-python
	pytest -vv py
