import time
from collections import defaultdict, deque
from concurrent.futures import Future
from enum import Enum
from json import dumps
from typing import (
    Any,
    Deque,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)

from arroyo.backends.kafka.configuration import build_kafka_configuration
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.types import BrokerValue, Topic


class UsageUnit(Enum):
    SECONDS = "seconds"
    BYTES = "bytes"
    BYTES_SEC = "bytes_sec"


UsageKey = Tuple[int, str, str, UsageUnit]

DEFAULT_TOPIC_NAME = "resources_usage_log"
DEFAULT_QUEUE_SIZE = 10000


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

    def __init__(
        self,
        granularity_sec: int,
        topic_name: str = DEFAULT_TOPIC_NAME,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        bootstrap_servers: Optional[Sequence[str]] = None,
        default_kafka_config: Optional[Mapping[str, Any]] = None,
        producer: Optional[KafkaProducer] = None,
    ) -> None:
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

        self.__topic = Topic(topic_name)

        if producer is not None:
            assert (
                bootstrap_servers is None and default_kafka_config is None
            ), (
                "If producer is provided, initialization "
                "parameters cannot be provided"
            )
            self.__producer: KafkaProducer = producer

        else:
            assert (
                bootstrap_servers is not None
                and default_kafka_config is not None
            ), (
                "If no producer is provided, initialization parameters "
                "have to be provided"
            )

            self.__producer = KafkaProducer(
                build_kafka_configuration(
                    default_config=default_kafka_config,
                    bootstrap_servers=bootstrap_servers,
                )
            )

        self.__queue_size = queue_size
        self.__futures: Deque[Future[BrokerValue[KafkaPayload]]] = deque()

    def record(
        self,
        resource_id: str,
        app_feature: str,
        amount: float,
        usage_type: UsageUnit,
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
        self.__prune_queue()

        for key, amount in self.__usage_batch.items():
            if len(self.__futures) == self.__queue_size:
                # TODO: Log here
                break

            message = {
                "timestamp": key[0],
                "shared_resource_id": key[1],
                "app_feature": key[2],
                "usage_unit": key[3].value,
                "amount": amount,
            }

            result = self.__producer.produce(
                self.__topic,
                KafkaPayload(
                    key=None, value=dumps(message).encode("utf-8"), headers=[]
                ),
            )
            self.__futures.append(result)

    def __prune_queue(self) -> None:
        while self.__futures and self.__futures[0].done():
            self.__futures.popleft()
