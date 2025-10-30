# usage-accountant
A library the Sentry application uses to account for usage of shared system
resources broken down by feature.

The library is as simple as a thin wrapper around a Kafka producer. It exists
only to ensure consistency in what we produce from different system and to
ensure we cap the amount of messages per second.

We have systems that need to track shared resources usage per feature built
in different language. This repo contains all of them in separate directories.

## Releasing

Images will be built and pushed by GHA -> GCP Cloud Build when PRs are merged into master.

This image sha can be set in [`usage-accountant/_values.yaml`](https://github.com/getsentry/ops/blob/master/k8s/services/usage-accountant/_values.yaml#L1) or overridden in a region override and applied with the [usage-accountant k8s pipeline](https://deploy.getsentry.net/go/pipelines#!/usage) or sentry-kube.
