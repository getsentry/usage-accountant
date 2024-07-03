from abc import ABC, abstractmethod
from typing import Dict, Any, NamedTuple, Sequence
from usageaccountant import UsageUnit


class Datapoint(NamedTuple):
    resource_id: str
    app_feature: str
    amount: int
    unit: UsageUnit


class Fetcher(ABC):
    def __init__(self, **kwargs: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def get(self) -> Sequence[Datapoint]:
        raise NotImplementedError
