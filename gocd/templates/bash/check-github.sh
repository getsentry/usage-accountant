#!/bin/bash

checks-githubactions-checkruns \
  --timeout-mins 60 \
  getsentry/usage-accountant \
  ${GO_REVISION_USAGE_ACCOUNTANT_REPO} \
  "Build and push production image"
