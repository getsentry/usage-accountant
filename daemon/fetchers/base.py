from abc import ABC, abstractmethod
from typing import Dict, Any, NamedTuple
from enum import Enum


# TODO: figure out file layout, remove, import from accumulator
class UsageUnit(Enum):
    MILLISECONDS = "milliseconds"
    BYTES = "bytes"
    MILLISECONDS_SEC = "milliseconds_sec"


class UsageKey(NamedTuple):
    timestamp: int
    resource_id: str
    app_feature: str
    unit: UsageUnit


class Fetcher(ABC):
    def __init__(self, **kwargs: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def get(self) -> "UsageKey":
        pass
