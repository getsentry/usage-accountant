local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

// The return value of this function is the body of a GoCD pipeline, rendered
// per region by pipedream. More information on the gocd-flavor YAML this
// produces can be found here:
// - https://github.com/tomzo/gocd-yaml-config-plugin#pipeline
// - https://www.notion.so/sentry/GoCD-New-Service-Quickstart-6d8db7a6964049b3b0e78b8a4b52e25d
function(region) {
  environment_variables: {
    SENTRY_REGION: region,
  },
  lock_behavior: 'unlockWhenFinished',
  materials: {
    usage_accountant_repo: {
      git: 'git@github.com:getsentry/usage-accountant.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'usage-accountant',
    },
  },
  stages: [
    {
      checks: {
        fetch_materials: true,
        jobs: {
          checks: {
            timeout: 1800,
            elastic_profile_id: 'usage-accountant',
            environment_variables: {
              // Required for checkruns.
              GITHUB_TOKEN: '{{SECRET:[devinfra-github][token]}}',
            },
            tasks: [
              gocdtasks.script(importstr '../bash/check-github.sh'),
            ],
          },
        },
      },
    },
    {
      deploy: {
        fetch_materials: true,
        jobs: {
          deploy: {
            timeout: 1200,
            elastic_profile_id: 'usage-accountant',
            tasks: [
              gocdtasks.script(importstr '../bash/deploy.sh'),
            ],
          },
        },
      },
    },
  ],
}
