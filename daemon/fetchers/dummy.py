from typing import Sequence
from .base import Fetcher, UsageUnit, Datapoint


class Dummy(Fetcher):
    def __init__(self, name: str = None) -> None:
        self._name = name

    async def get(self) -> Sequence[Datapoint]:
        return [Datapoint(self._name, "dummy", 1, UsageUnit.MILLISECONDS)]
