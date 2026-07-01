# usage-accountant GoCD pipelines

usage-accountant is deployed by GoCD on each `main` commit. The pipelines are
generated from Jsonnet using the [gocd-jsonnet](https://github.com/getsentry/gocd-jsonnet)
`pipedream` library, which fans the pipeline out across every region the
service runs in (`us`, `us2`, `de`, `s4s2`, the single-tenant regions, and
`control`).

Each region pipeline runs two stages:

1. **checks** — waits for the `Build and push production image` GitHub Actions
   check run for the deployed commit (the image built by
   `.github/workflows/image.yaml`).
2. **deploy** — patches the `usage-accountant` CronJob image to that commit via
   `k8s-deploy --type=cronjob`, so the next scheduled run uses it.

This replaces the previous flow of manually bumping `image_tag` in
`getsentry/ops`.

## Rendering

Generated pipelines are not committed; CI renders and validates them via
`getsentry/action-gocd-jsonnet`. To render locally:

```sh
brew install go-jsonnet jsonnet-bundler yq
make gocd
```

Output is written to `gocd/generated-pipelines/` (gitignored).

## Files

- `gocd/templates/usage-accountant.jsonnet` — entry point / pipedream config.
- `gocd/templates/pipelines/usage-accountant.libsonnet` — per-region pipeline body.
- `gocd/templates/bash/` — the check and deploy scripts.
