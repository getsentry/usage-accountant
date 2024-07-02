from .base import Fetcher, UsageKey, UsageUnit


class Dummy(Fetcher):
    def __init__(self, name: str = None) -> None:
        self._name = name

    async def get(self) -> "UsageKey":
        return UsageKey(0.1, self._name, "dummy", UsageUnit.MILLISECONDS)
