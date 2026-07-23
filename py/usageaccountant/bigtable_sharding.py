"""
Bigtable-backed :class:`QuerySharder`.

Uses Bigtable's ``SampleRowKeys`` API to sample row keys that are roughly
equally spaced throughout the table. These samples are turned into row-key
ranges which are used as bounds for the sharded query.
"""

import logging
import re
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

from google.cloud import bigquery

from usageaccountant.query_sharding import QuerySharder, ShardedQuery

logger = logging.getLogger("bigtable_sharding")

# Token in a shardable query that is replaced with the per-shard row-key
# predicate (see ``build_shard_clause``). Placed in a ``WHERE TRUE`` context so
# the empty (single-shard) case is still valid SQL.
DEFAULT_SHARD_PLACEHOLDER = "{shard_predicate}"
# BigQuery column exposing the Bigtable row key (the external table is created
# with ``read_rowkey_as_string = true``, so it is a ``STRING``).
DEFAULT_ROWKEY_COLUMN = "rowkey"

# Given a ``bigtable_shard`` config, returns the raw row-key samples that split
# the table into roughly equal-sized shards. Injectable so tests can supply
# boundaries without a live Bigtable (default hits the Data API).
Sampler = Callable[[Mapping[str, Any]], Sequence[bytes]]


def default_sampler(
    shard_config: Mapping[str, Any]
) -> Sequence[bytes]:  # pragma: no cover
    """
    Fetches Bigtable row-key samples via the Data API ``SampleRowKeys`` RPC.
    """
    import google.cloud.bigtable as bigtable_module

    # Cast the module to Any: its typing is partial and it is absent from the
    # type-check env, so this keeps mypy --strict happy either way without an
    # env-dependent ``type: ignore``. The import is written as
    # ``import google.cloud.bigtable`` rather than ``from google.cloud import
    # bigtable`` so that, when the package is absent, mypy treats it as a
    # missing module (silenced via mypy.ini) rather than a missing attribute of
    # the ``google.cloud`` namespace package (an [attr-defined] error that
    # ``ignore_missing_imports`` does not suppress).
    bigtable = cast(Any, bigtable_module)
    client = bigtable.Client(project=shard_config.get("project"), admin=False)
    instance = client.instance(shard_config["instance"])
    table = instance.table(shard_config["table"])
    return [sample.row_key for sample in table.sample_row_keys()]


def shard_ranges_from_samples(
    sample_keys: Sequence[bytes],
) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    Turns the row-key samples returned by Bigtable's ``SampleRowKeys`` into a
    list of half-open ``[start, end)`` row-key ranges that together cover the
    whole table.

    ``N`` distinct interior boundaries yield ``N + 1`` ranges; the first range
    is unbounded below (``start=None``) and the last unbounded above
    (``end=None``). Boundaries are decoded as UTF-8 (the external table reads
    the row key as a ``STRING``); a non-UTF-8 boundary is dropped, which merges
    the two adjacent shards -- coarser, but still correct and complete. With no
    usable boundaries a single whole-table range is returned.
    """
    boundaries: List[str] = []
    for key in sample_keys:
        # The final sample is an empty key marking end-of-table; it is not a
        # usable boundary.
        if not key:
            continue
        try:
            boundaries.append(key.decode("utf-8"))
        except UnicodeDecodeError:
            logger.warning("skipping non-UTF-8 row-key sample")

    ordered = sorted(set(boundaries))
    if not ordered:
        return [(None, None)]

    ranges: List[Tuple[Optional[str], Optional[str]]] = []
    prev: Optional[str] = None
    for boundary in ordered:
        ranges.append((prev, boundary))
        prev = boundary
    ranges.append((prev, None))
    return ranges


def build_shard_clause(
    start: Optional[str], end: Optional[str], column: str
) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
    """
    Builds the SQL fragment (and its bound parameters) restricting a query to a
    single ``[start, end)`` row-key range.

    Row-key bounds are passed as named query parameters rather than
    interpolated into the SQL, so arbitrary row-key bytes cannot break out of
    the query. The returned fragment is prefixed with `` AND `` so it can be
    dropped into a ``WHERE TRUE {shard_predicate}`` context.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
        raise ValueError(f"invalid rowkey column name: {column!r}")

    conditions: List[str] = []
    params: List[bigquery.ScalarQueryParameter] = []
    if start is not None:
        conditions.append(f"{column} >= @shard_start")
        params.append(
            bigquery.ScalarQueryParameter("shard_start", "STRING", start)
        )
    if end is not None:
        conditions.append(f"{column} < @shard_end")
        params.append(
            bigquery.ScalarQueryParameter("shard_end", "STRING", end)
        )

    clause = "".join(f" AND {condition}" for condition in conditions)
    return clause, params


class BigtableQuerySharder(QuerySharder):
    """
    Shards a query over the row-key ranges of a Bigtable-backed table.

    The shard boundaries come from ``sampler`` (``SampleRowKeys``); it defaults
    to the live Bigtable Data API client but is injectable so tests can supply
    boundaries without a live Bigtable.
    """

    def __init__(self, sampler: Optional[Sampler] = None) -> None:
        self.sampler = sampler if sampler is not None else default_sampler

    def validate_config(
        self, shard_config: Mapping[str, Any], query: str
    ) -> None:
        """
        A ``bigtable_shard`` config must name the ``instance`` and ``table`` to
        sample, and ``query`` must contain the shard placeholder so the
        per-shard predicate can be injected.
        """
        assert (
            "instance" in shard_config
        ), "bigtable_shard requires an 'instance'"
        assert "table" in shard_config, "bigtable_shard requires a 'table'"
        placeholder = shard_config.get(
            "placeholder", DEFAULT_SHARD_PLACEHOLDER
        )
        assert (
            placeholder in query
        ), f"sharded query must contain the {placeholder!r} placeholder"

    def build_shard_queries(
        self, query: str, shard_config: Mapping[str, Any]
    ) -> List[ShardedQuery]:
        """
        Samples the table's row keys and, for each ``[start, end)`` range,
        replaces the shard placeholder with a row-key predicate bounding the
        query to that range.
        """
        placeholder = shard_config.get(
            "placeholder", DEFAULT_SHARD_PLACEHOLDER
        )
        column = shard_config.get("rowkey_column", DEFAULT_ROWKEY_COLUMN)

        ranges = shard_ranges_from_samples(self.sampler(shard_config))

        shard_queries: List[ShardedQuery] = []
        for start, end in ranges:
            clause, params = build_shard_clause(start, end, column)
            shard_queries.append(
                ShardedQuery(
                    sql=query.replace(placeholder, clause),
                    parameters=params,
                    description=f"[{start!r}, {end!r})",
                )
            )
        return shard_queries
