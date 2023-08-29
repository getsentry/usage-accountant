import time
from collections import defaultdict
from enum import Enum
from typing import MutableMapping, Optional, Tuple


class UsageType(Enum):
    SECONDS = "seconds"
    BYTES = "bytes"
    BYTES_SEC = "bytes_sec"


UsageKey = Tuple[int, str, str, UsageType]


class UsageAccumulator:
    """
    Records the usage of shared resources. This library is meant to
    produce observability data on how much shared resources are used
    by different features.

    Data is accumulated locally and produced to Kafka periodically
    to reduce the volume impact on Kafka when a large number of pods
    use this api.

    In order for accumulation to be effective and preserve the Kafka
    consumers that will consume from this, instances of this should be
    shared and kept around for as long as possible.

    A good idea is to create an instance at the beginning of your
    program and keep reusing that till you exit.

    Resources are identified as `resource_id` and feature as
    `app_feature` in the methods. These cannot be enum otherwise we would
    have to re-release the api every time we add one. And we do not
    have a single source of truth for them anyway.
    """

    def __init__(self, granularity_sec: int) -> None:
        """
        Initializes the accumulator. Instances should be kept around
        as much as possible as they preserve the instance of the
        Kafka producers, with it the connections.

        `granularity_sec` defines how often the accumulator will be
        flushed.
        """
        self.__first_timestamp: Optional[float] = None
        self.__usage_batch: MutableMapping[UsageKey, float] = defaultdict()
        self.__granularity_sec = granularity_sec

    def record(
        self,
        resource_id: str,
        app_feature: str,
        amount: float,
        usage_type: UsageType,
    ) -> None:
        """
        Record a chunk of usage of a resource for a feature.

        This method is not a blocking one and it is cheap on
        the main thread. So feel free to call it often.
        Specifically it takes the system time to associate to
        the usage to a time range.

        `resource_id` identifies the shared resource.
          Example: `generic_metrics_indexer_ingestion`
        `app_feature` identifies the product feature.
        `amount`  is the amount of resource used.
        `usage_type` is the unit of measure for `amount`.
        """
        now = time.time()
        if self.__first_timestamp is None:
            self.__first_timestamp = now

        self.__usage_batch[
            (
                int(now / self.__granularity_sec),
                resource_id,
                app_feature,
                usage_type,
            )
        ] += amount

        if now - self.__first_timestamp > self.__granularity_sec:
            self.flush(synchronous=False)

    def flush(self, synchronous: bool = True) -> None:
        """
        This method is blocking and it forces the api to flush
        data accumulated to Kafka.

        This method is supposed to be used when we are shutting
        down the program that was accumulating data.
        """
        pass
