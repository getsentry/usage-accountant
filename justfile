default: test-python

setup-pre-commit:
  pre-commit install --install-hooks

[working-directory('py')]
test-python:
  echo Testing python...
  uv run pytest
