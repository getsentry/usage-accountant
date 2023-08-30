from json import loads
from typing import Optional
from unittest import mock

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import BrokerValue, Partition, Topic
from arroyo.utils.clock import TestingClock

from usageaccountant import UsageAccumulator, UsageUnit


@pytest.fixture
def broker() -> LocalBroker[KafkaPayload]:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, TestingClock())
    topic = Topic("test_resource_usage")
    broker.create_topic(topic, 1)
    return broker


def assert_msg(
    message: Optional[BrokerValue[KafkaPayload]],
    timestamp: int,
    resource_id: str,
    app_feature: str,
    amount: float,
    usage_type: UsageUnit,
) -> None:
    assert message is not None
    payload = message.payload
    assert payload is not None
    formatted = loads(payload.value.decode("utf-8"))
    assert formatted == {
        "timestamp": timestamp,
        "shared_resource_id": resource_id,
        "app_feature": app_feature,
        "usage_unit": usage_type.value,
        "amount": amount,
    }


@mock.patch("time.time")
def test_two_buckets(
    mock_time: mock.Mock, broker: LocalBroker[KafkaPayload]
) -> None:
    producer = broker.get_producer()
    accumulator = UsageAccumulator(
        10, topic_name="test_resource_usage", producer=producer
    )

    mock_time.return_value = 1594839910.1
    accumulator.record(
        resource_id="metrics_consumer",
        app_feature="spans",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )
    accumulator.record(
        resource_id="metrics_consumer",
        app_feature="transactions",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )
    mock_time.return_value = 1594839920.1
    accumulator.record(
        resource_id="metrics_consumer",
        app_feature="spans",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )
    accumulator.record(
        resource_id="metrics_consumer",
        app_feature="spans",
        amount=10.0,
        usage_type=UsageUnit.BYTES,
    )

    accumulator.flush()

    topic = Topic("test_resource_usage")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert_msg(
        msg1, 1594839910, "metrics_consumer", "spans", 10.0, UsageUnit.BYTES
    )
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert_msg(
        msg2,
        1594839910,
        "metrics_consumer",
        "transactions",
        10.0,
        UsageUnit.BYTES,
    )
    msg3 = broker.consume(Partition(topic, 0), 2)
    assert_msg(
        msg3, 1594839920, "metrics_consumer", "spans", 20.0, UsageUnit.BYTES
    )
    assert broker.consume(Partition(topic, 0), 3) is None


@mock.patch("time.time")
def test_buffer_overflow(
    mock_time: mock.Mock, broker: LocalBroker[KafkaPayload]
) -> None:
    producer = broker.get_producer()
    accumulator = UsageAccumulator(
        10, topic_name="test_resource_usage", producer=producer
    )
    mock_time.return_value = 1594839910.1

    for i in range(0, 9500):
        accumulator.record(
            resource_id="metrics_consumer",
            app_feature=f"spans_{i}",
            amount=10.0,
            usage_type=UsageUnit.BYTES,
        )
    accumulator.flush()

    topic = Topic("test_resource_usage")
    msg1 = broker.consume(Partition(topic, 0), 9499)
    assert msg1 is not None
    # Offset 9500 would have been the last message we passed to
    # the accumulator. It is not produced for buffer overflow.
    msg2 = broker.consume(Partition(topic, 0), 9500)
    assert msg2 is None
