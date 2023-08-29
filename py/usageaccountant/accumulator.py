from enum import Enum


class UsageType(Enum):
    SECONDS = "seconds"
    BYTES = "bytes"
    BYTES_SEC = "bytes_sec"


class UsageAccumulator:
    def __init__(self, buffer_size: int) -> None:
        pass

    def record(
        self,
        resource_id: str,
        app_feature: str,
        amount: float,
        usage_type: UsageType,
    ) -> None:
        pass

    def flush(self) -> None:
        pass
