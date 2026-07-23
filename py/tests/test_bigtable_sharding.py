import unittest
from typing import Any, Mapping, Sequence

from usageaccountant import bigtable_sharding as bts
from usageaccountant.bigtable_sharding import BigtableQuerySharder

# A shardable query: the placeholder sits in a ``WHERE TRUE`` context so the
# single-shard (empty predicate) case is still valid SQL.
SHARD_QUERY = (
    "SELECT app_feature, amount FROM t "
    "WHERE TRUE {shard_predicate} GROUP BY app_feature"
)


def sampler_gm(_config: Mapping[str, Any]) -> Sequence[bytes]:
    """Two interior boundaries -> three shards."""
    return [b"g", b"m"]


class TestValidateConfig(unittest.TestCase):
    def test_ok(self) -> None:
        BigtableQuerySharder().validate_config(
            {"instance": "objectstore", "table": "objectstore"}, SHARD_QUERY
        )

    def test_missing_instance(self) -> None:
        with self.assertRaises(AssertionError):
            BigtableQuerySharder().validate_config(
                {"table": "objectstore"}, SHARD_QUERY
            )

    def test_missing_table(self) -> None:
        with self.assertRaises(AssertionError):
            BigtableQuerySharder().validate_config(
                {"instance": "objectstore"}, SHARD_QUERY
            )

    def test_missing_placeholder(self) -> None:
        with self.assertRaises(AssertionError):
            BigtableQuerySharder().validate_config(
                {"instance": "objectstore", "table": "objectstore"},
                "SELECT app_feature, amount FROM t",
            )


class TestShardRangesFromSamples(unittest.TestCase):
    def test_empty(self) -> None:
        assert bts.shard_ranges_from_samples([]) == [(None, None)]

    def test_only_end_sentinel(self) -> None:
        # The trailing empty key marks end-of-table, not a usable boundary.
        assert bts.shard_ranges_from_samples([b""]) == [(None, None)]

    def test_basic_sorted_and_bounded(self) -> None:
        # Unsorted input, with the end sentinel, yields sorted half-open
        # ranges spanning (None -> ... -> None).
        assert bts.shard_ranges_from_samples([b"m", b"g", b""]) == [
            (None, "g"),
            ("g", "m"),
            ("m", None),
        ]

    def test_dedup(self) -> None:
        assert bts.shard_ranges_from_samples([b"g", b"g", b"m"]) == [
            (None, "g"),
            ("g", "m"),
            ("m", None),
        ]

    def test_skips_non_utf8(self) -> None:
        # A non-UTF-8 boundary is dropped (merging its two shards).
        assert bts.shard_ranges_from_samples([b"\xff\xfe", b"m"]) == [
            (None, "m"),
            ("m", None),
        ]


class TestBuildShardClause(unittest.TestCase):
    def test_both_bounds(self) -> None:
        clause, params = bts.build_shard_clause("g", "m", "rowkey")
        assert clause == (
            " AND rowkey >= @shard_start AND rowkey < @shard_end"
        )
        assert [(p.name, p.value) for p in params] == [
            ("shard_start", "g"),
            ("shard_end", "m"),
        ]

    def test_start_only(self) -> None:
        clause, params = bts.build_shard_clause("m", None, "rowkey")
        assert clause == " AND rowkey >= @shard_start"
        assert [(p.name, p.value) for p in params] == [("shard_start", "m")]

    def test_end_only(self) -> None:
        clause, params = bts.build_shard_clause(None, "g", "rowkey")
        assert clause == " AND rowkey < @shard_end"
        assert [(p.name, p.value) for p in params] == [("shard_end", "g")]

    def test_unbounded(self) -> None:
        clause, params = bts.build_shard_clause(None, None, "rowkey")
        assert clause == ""
        assert params == []

    def test_invalid_column(self) -> None:
        with self.assertRaises(ValueError):
            bts.build_shard_clause("g", "m", "rowkey; DROP TABLE t")


class TestBuildShardQueries(unittest.TestCase):
    def test_defaults_to_live_sampler(self) -> None:
        assert BigtableQuerySharder().sampler is bts.default_sampler

    def test_predicate_per_shard(self) -> None:
        sharder = BigtableQuerySharder(sampler=sampler_gm)
        shard_queries = sharder.build_shard_queries(
            SHARD_QUERY, {"instance": "i", "table": "t"}
        )
        assert len(shard_queries) == 3

        first = shard_queries[0]
        assert "{shard_predicate}" not in first.sql
        assert "rowkey < @shard_end" in first.sql
        assert "rowkey >= @shard_start" not in first.sql
        assert first.description == "[None, 'g')"

        mid = shard_queries[1]
        assert "rowkey >= @shard_start" in mid.sql
        assert "rowkey < @shard_end" in mid.sql
        assert [(p.name, p.value) for p in mid.parameters] == [
            ("shard_start", "g"),
            ("shard_end", "m"),
        ]
        assert mid.description == "['g', 'm')"

        last = shard_queries[2]
        assert "rowkey >= @shard_start" in last.sql
        assert "rowkey < @shard_end" not in last.sql
        assert last.description == "['m', None)"


if __name__ == "__main__":
    unittest.main()
