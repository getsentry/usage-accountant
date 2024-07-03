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
class in `fetchers` directory. use dummy.py as example.

All you need is to add another file, say `another.py`
With fetcher class in it:
```
from typing import Sequence
from .base import Fetcher, UsageUnit, Datapoint


class Another(Fetcher):
    async def get(self) -> Sequence[Datapoint]:
      ... your fetcher logic ...

```

and you can start using it by setting in config

```
fetchers:
- type: another
  period: 5
```