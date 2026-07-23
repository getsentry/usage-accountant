import unittest
from datetime import date, datetime, timezone
from io import StringIO
from json import dumps, loads
from typing import Any, Iterable, List, Mapping, Sequence, cast
from unittest.mock import Mock

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Partition, Topic
from arroyo.utils.clock import MockedClock
from google.cloud import bigquery

from usageaccountant import accumulator
from usageaccountant import bigquery_fetcher as bqf
from usageaccountant.accumulator import UsageUnit
from usageaccountant.fetcher_utils import UsageAccumulatorRecord
from usageaccountant.query_sharding import QuerySharder, ShardedQuery

# A day-aligned epoch (2026-06-11 00:00:00 GMT).
INVENTORY_TS = 1781136000


class MockBQ:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self.rows = rows

    def query(self, query: str) -> Mock:
        result_fn = Mock(return_value=self.rows)
        return Mock(result=result_fn)


class ShardedMockBQ:
    """
    Returns one preset batch of rows per successive ``query`` call and records
    the (query, parameters) of each call for assertions.
    """

    def __init__(self, batches: Sequence[List[Mapping[str, Any]]]):
        self.batches = list(batches)
        self.calls: List[Any] = []

    def query(self, query: str, job_config: Any = None) -> Mock:
        params = list(job_config.query_parameters) if job_config else []
        self.calls.append((query, params))
        rows = self.batches.pop(0)
        return Mock(result=Mock(return_value=rows))


class FakeSharder(QuerySharder):
    """
    A backend-agnostic sharder returning preset shard queries, so the fetcher's
    orchestration can be tested without any real sharding backend.
    """

    def __init__(self, shard_queries: List[ShardedQuery]) -> None:
        self.shard_queries = shard_queries

    def validate_config(
        self, shard_config: Mapping[str, Any], query: str
    ) -> None:
        pass

    def build_shard_queries(
        self, query: str, shard_config: Mapping[str, Any]
    ) -> List[ShardedQuery]:
        return self.shard_queries


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

    def test_normalize_timestamp_none(self) -> None:
        assert bqf.normalize_timestamp(None) is None

    def test_normalize_timestamp_int(self) -> None:
        assert bqf.normalize_timestamp(INVENTORY_TS) == INVENTORY_TS

    def test_normalize_timestamp_float(self) -> None:
        assert bqf.normalize_timestamp(INVENTORY_TS + 0.9) == INVENTORY_TS

    def test_normalize_timestamp_datetime(self) -> None:
        dt = datetime(2026, 6, 11, tzinfo=timezone.utc)
        assert bqf.normalize_timestamp(dt) == INVENTORY_TS

    def test_normalize_timestamp_date(self) -> None:
        assert bqf.normalize_timestamp(date(2026, 6, 11)) == INVENTORY_TS

    def test_normalize_timestamp_bool_rejected(self) -> None:
        with self.assertRaises(TypeError):
            bqf.normalize_timestamp(True)

    def test_normalize_timestamp_unsupported_type(self) -> None:
        with self.assertRaises(TypeError):
            bqf.normalize_timestamp("2026-06-11")

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
            UsageAccumulatorRecord(
                "gcs_objectstore",
                "attachments",
                100,
                UsageUnit.BYTES,
                INVENTORY_TS,
            ),
            UsageAccumulatorRecord(
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
            UsageAccumulatorRecord(
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
            UsageAccumulatorRecord(
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
            bq_client=cast(bigquery.Client, client),
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
            bq_client=cast(bigquery.Client, client),
            dry_run=True,
        )
        # Nothing is produced in dry-run mode.
        assert self.broker.consume(Partition(self.topic, 0), 0) is None

    # --- Section: Bigtable row-key sharding ---

    shard_query = (
        "SELECT app_feature, amount FROM t "
        "WHERE TRUE {shard_predicate} GROUP BY app_feature"
    )

    def shard_query_file(self) -> str:
        return dumps(
            [
                {
                    "query": self.shard_query,
                    "unit": "bytes",
                    "shared_resource_id": "bigtable_objectstore",
                    "bigtable_shard": {
                        "instance": "objectstore",
                        "table": "objectstore",
                    },
                }
            ]
        )

    def test_parse_shard_ok(self) -> None:
        assert bqf.parse_and_assert_query_file(
            StringIO(self.shard_query_file())
        )

    def test_parse_shard_invalid_delegates(self) -> None:
        # bigtable_shard validation is delegated to bigtable_sharding; a
        # config missing a required key must still fail the parse.
        query_file = dumps(
            [
                {
                    "query": self.shard_query,
                    "unit": "bytes",
                    "shared_resource_id": "x",
                    "bigtable_shard": {"table": "objectstore"},
                }
            ]
        )
        with self.assertRaises(AssertionError):
            bqf.parse_and_assert_query_file(StringIO(query_file))

    def test_aggregate_records_sums_across_shards(self) -> None:
        records = [
            UsageAccumulatorRecord(
                "bigtable_objectstore", "attachments", 10, UsageUnit.BYTES, 7
            ),
            UsageAccumulatorRecord(
                "bigtable_objectstore", "attachments", 20, UsageUnit.BYTES, 7
            ),
            UsageAccumulatorRecord(
                "bigtable_objectstore", "profiles", 5, UsageUnit.BYTES, 7
            ),
        ]
        aggregated = bqf.aggregate_records(records)
        by_feature = {r.app_feature: r.amount for r in aggregated}
        assert by_feature == {"attachments": 30, "profiles": 5}

    def test_run_sharded_query_runs_each_shard(self) -> None:
        # run_sharded_query is backend-agnostic: it runs each ShardedQuery the
        # sharder hands it (sql + parameters) and concatenates the rows.
        shards = [
            ShardedQuery("SELECT shard_a", [], "[None, 'g')"),
            ShardedQuery(
                "SELECT shard_b",
                [
                    bigquery.ScalarQueryParameter(
                        "shard_start", "STRING", "g"
                    ),
                    bigquery.ScalarQueryParameter("shard_end", "STRING", "m"),
                ],
                "['g', 'm')",
            ),
            ShardedQuery("SELECT shard_c", [], "['m', None)"),
        ]
        client = ShardedMockBQ(
            [
                [{"app_feature": "a", "amount": 1}],
                [{"app_feature": "a", "amount": 2}],
                [{"app_feature": "b", "amount": 3}],
            ]
        )
        rows = list(
            bqf.run_sharded_query(
                cast(bigquery.Client, client),
                "unused",
                {"any": "config"},
                FakeSharder(shards),
            )
        )
        assert rows == [
            {"app_feature": "a", "amount": 1},
            {"app_feature": "a", "amount": 2},
            {"app_feature": "b", "amount": 3},
        ]
        # Each shard's sql and parameters reach the client, in order.
        assert [query for query, _ in client.calls] == [
            "SELECT shard_a",
            "SELECT shard_b",
            "SELECT shard_c",
        ]
        assert [(p.name, p.value) for p in client.calls[1][1]] == [
            ("shard_start", "g"),
            ("shard_end", "m"),
        ]

    def test_main_sharded_aggregates_and_produces(self) -> None:
        client = ShardedMockBQ(
            [
                [{"app_feature": "attachments", "amount": "10"}],
                # attachments straddles a shard boundary.
                [{"app_feature": "attachments", "amount": "20"}],
                [{"app_feature": "profiles", "amount": "5"}],
            ]
        )
        shards = [
            ShardedQuery(f"SELECT shard_{i}", [], f"shard {i}")
            for i in range(3)
        ]
        bqf.main(
            query_file=StringIO(self.shard_query_file()),
            usage_accumulator=self.usage_accumulator,
            bq_client=cast(bigquery.Client, client),
            dry_run=False,
            sharders={"bigtable_shard": FakeSharder(shards)},
        )

        by_feature = {}
        index = 0
        while True:
            msg = self.broker.consume(Partition(self.topic, 0), index)
            if msg is None:
                break
            payload = loads(msg.payload.value.decode("utf-8"))
            by_feature[payload["app_feature"]] = payload
            index += 1

        assert by_feature["attachments"]["amount"] == 30
        assert by_feature["attachments"]["shared_resource_id"] == (
            "bigtable_objectstore"
        )
        assert by_feature["profiles"]["amount"] == 5
