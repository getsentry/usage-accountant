#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

# usage-accountant runs as a CronJob in the sentry-system namespace; patch its
# image to the built commit so the next scheduled run uses it.
/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --type="cronjob" \
  --label-selector="service=usage-accountant" \
  --image="us-central1-docker.pkg.dev/sentryio/usage-accountant/image:${GO_REVISION_USAGE_ACCOUNTANT_REPO}" \
  --container-name="usage-accountant" \
  --namespace="sentry-system"
