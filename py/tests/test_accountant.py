from json import loads

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Partition, Topic
from arroyo.utils.clock import TestingClock

from usageaccountant import UsageAccumulator, UsageUnit


def test_broker() -> None:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, TestingClock())
    topic = Topic("test_resource_usage")
    broker.create_topic(topic, 1)

    producer = broker.get_producer()

    accumulator = UsageAccumulator(
        10, topic_name="test_resource_usage", producer=producer
    )

    accumulator.record(
        resource_id="generic_metrics_consumer",
        app_feature="spans",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )
    accumulator.record(
        resource_id="generic_metrics_consumer",
        app_feature="spans",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )
    accumulator.flush()

    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None
    payload = msg1.payload
    assert payload is not None
    formatted = loads(payload.value.decode("utf-8"))
    assert formatted["shared_resource_id"] == "generic_metrics_consumer"
    assert formatted["app_feature"] == "spans"
    assert formatted["usage_unit"] == UsageUnit.BYTES.value
    assert formatted["amount"] == 20.0

    assert broker.consume(Partition(topic, 0), 1) is None
