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

# Render the GoCD deploy pipelines from the jsonnet templates. Requires
# go-jsonnet, jsonnet-bundler and yq (see gocd/README.md). CI renders these
# too via getsentry/action-gocd-jsonnet, so generated output is not committed.
.PHONY: gocd
gocd:
	rm -rf ./gocd/generated-pipelines
	mkdir -p ./gocd/generated-pipelines
	cd ./gocd/templates && jb install && jb update
	find ./gocd -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnetfmt -i
	cd ./gocd/templates && jsonnet --ext-code output-files=true -J vendor -m ../generated-pipelines ./usage-accountant.jsonnet
	cd ./gocd/generated-pipelines && find . -type f -name '*.yaml' -print0 | xargs -n 1 -0 yq -p json -o yaml -i
