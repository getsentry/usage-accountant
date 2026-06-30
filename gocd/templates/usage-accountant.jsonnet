local pipeline = import './pipelines/usage-accountant.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

// usage-accountant is deployed to us, us2, de, s4s2, the single-tenant
// regions, and control. control is excluded by pipedream by default, so it
// is added back explicitly via include_regions.
local pipedream_config = {
  name: 'usage-accountant',
  materials: {
    usage_accountant_repo: {
      git: 'git@github.com:getsentry/usage-accountant.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'usage-accountant',
    },
  },
  rollback: {
    material_name: 'usage_accountant_repo',
    stage: 'deploy',
    elastic_profile_id: 'usage-accountant',
  },
  include_regions: ['control'],

  // Auto-deploy each main commit once its image build check passes.
  auto_deploy: true,
};

pipedream.render(pipedream_config, pipeline)
