import unittest
from io import StringIO
from json import loads
from typing import Any, Iterable, List, Mapping
from unittest.mock import Mock

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Partition, Topic
from arroyo.utils.clock import MockedClock

from usageaccountant import accumulator
from usageaccountant import bigquery_fetcher as bqf
from usageaccountant.accumulator import UsageUnit

# A day-aligned epoch (2026-06-11 00:00:00 GMT).
INVENTORY_TS = 1781136000


class MockBQ:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self.rows = rows

    def query(self, query: str) -> Mock:
        result_fn = Mock(return_value=self.rows)
        return Mock(result=result_fn)


class TestBigQueryFetcher(unittest.TestCase):
    query_str = (
        '[{"query": "SELECT app_feature, amount FROM t", '
        '"unit": "bytes", "shared_resource_id": "gcs_objectstore"}]'
    )

    def setUp(self) -> None:
        storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
        self.broker = LocalBroker(storage, MockedClock())
        self.topic = Topic("test_bq_fetcher")
        self.broker.create_topic(self.topic, 1)

        producer = self.broker.get_producer()
        # granularity of 1s so an overridden timestamp is preserved verbatim.
        self.usage_accumulator = accumulator.UsageAccumulator(
            1, topic_name="test_bq_fetcher", producer=producer
        )

    # -- query file parsing -------------------------------------------------

    def test_parse_and_assert_query_file(self) -> None:
        assert bqf.parse_and_assert_query_file(StringIO(self.query_str))

    def test_parse_and_assert_query_file_empty(self) -> None:
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(StringIO("[]"))

    def test_parse_and_assert_query_file_not_list(self) -> None:
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(StringIO('{"query": "x"}'))

    def test_parse_and_assert_query_file_missing_query(self) -> None:
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(
                StringIO('[{"unit": "bytes", "shared_resource_id": "x"}]')
            )

    def test_parse_and_assert_query_file_missing_resource(self) -> None:
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(
                StringIO('[{"query": "x", "unit": "bytes"}]')
            )

    def test_parse_and_assert_query_file_missing_unit(self) -> None:
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(
                StringIO('[{"query": "x", "shared_resource_id": "y"}]')
            )

    def test_process_rows_with_timestamp(self) -> None:
        rows: List[Mapping[str, Any]] = [
            {
                "app_feature": "attachments",
                "amount": 100,
                "timestamp": INVENTORY_TS,
            },
            {
                "app_feature": "preprod",
                "amount": 200,
                "timestamp": INVENTORY_TS,
            },
        ]
        records = bqf.process_rows(rows, UsageUnit.BYTES, "gcs_objectstore")
        assert records == [
            bqf.UsageAccumulatorRecord(
                "gcs_objectstore",
                "attachments",
                100,
                UsageUnit.BYTES,
                INVENTORY_TS,
            ),
            bqf.UsageAccumulatorRecord(
                "gcs_objectstore",
                "preprod",
                200,
                UsageUnit.BYTES,
                INVENTORY_TS,
            ),
        ]

    def test_process_rows_without_timestamp(self) -> None:
        rows: List[Mapping[str, Any]] = [
            {"app_feature": "attachments", "amount": 100}
        ]
        records = bqf.process_rows(
            rows, UsageUnit.BYTES, "bigtable_objectstore"
        )
        assert records == [
            bqf.UsageAccumulatorRecord(
                "bigtable_objectstore",
                "attachments",
                100,
                UsageUnit.BYTES,
                None,
            )
        ]

    def test_process_rows_skips_nulls(self) -> None:
        rows: List[Mapping[str, Any]] = [
            {"app_feature": None, "amount": 100},
            {"app_feature": "attachments", "amount": None},
            {"app_feature": "preprod", "amount": 50},
        ]
        records = bqf.process_rows(rows, UsageUnit.BYTES, "gcs_objectstore")
        assert records == [
            bqf.UsageAccumulatorRecord(
                "gcs_objectstore", "preprod", 50, UsageUnit.BYTES, None
            )
        ]

    def test_main_produces_records(self) -> None:
        client = MockBQ(
            [
                {
                    "app_feature": "attachments",
                    "amount": "123",
                    "timestamp": INVENTORY_TS,
                }
            ]
        )

        bqf.main(
            query_file=StringIO(self.query_str),
            usage_accumulator=self.usage_accumulator,
            bq_client=client,
            dry_run=False,
        )

        msg = self.broker.consume(Partition(self.topic, 0), 0)
        assert msg is not None
        payload = loads(msg.payload.value.decode("utf-8"))
        assert payload == {
            "timestamp": INVENTORY_TS,
            "shared_resource_id": "gcs_objectstore",
            "app_feature": "attachments",
            "usage_unit": "bytes",
            "amount": 123,
        }
        assert self.broker.consume(Partition(self.topic, 0), 1) is None

    def test_main_dry_run(self) -> None:
        client = MockBQ(
            [
                {
                    "app_feature": "attachments",
                    "amount": "123",
                    "timestamp": INVENTORY_TS,
                }
            ]
        )
        bqf.main(
            query_file=StringIO(self.query_str),
            usage_accumulator=self.usage_accumulator,
            bq_client=client,
            dry_run=True,
        )
        # Nothing is produced in dry-run mode.
        assert self.broker.consume(Partition(self.topic, 0), 0) is None
