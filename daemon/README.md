# Usage Fetcher

Somewhat simple daemon that runs a set of periodical fetches from external
sources and feeds them to kafka via usage-accountant

## Config

General config structure

```
kafka:
  topic: str
  bootstrap_servers:
    - str
  config_params:
    key: value

fetchers:
- type: str
  period: float
  args:
    name: value
```

example in tests/config.yaml


## Modularity

To add more fetchers you need to add another implementation of `Fetcher`
class in `fetchers` directory.
see dummy.py as example.
