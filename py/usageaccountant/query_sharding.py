"""
Interface for query sharding.

BigQuery forcibly kills queries that run over 6 hours. Sharding allows a
long-running query to run to completion by breaking it into smaller pieces.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Mapping

from google.cloud import bigquery


@dataclass(frozen=True)
class ShardedQuery:
    """
    A single shard's fully-formed query: the SQL (with any shard placeholder
    already resolved) and the bound query parameters it references.

    ``description`` is a human-readable label for the shard, intended for
    logging (e.g. a Bigtable sharder labels it with the row-key range).
    """

    sql: str
    parameters: List[bigquery.ScalarQueryParameter]
    description: str


class QuerySharder(ABC):
    """
    Splits a query into per-shard queries for a particular storage backend.
    """

    @abstractmethod
    def validate_config(
        self, shard_config: Mapping[str, Any], query: str
    ) -> None:
        """
        Assert that ``shard_config`` is well-formed for this backend and that
        ``query`` is compatible with it (e.g. contains the shard placeholder).
        """
        raise NotImplementedError

    @abstractmethod
    def build_shard_queries(
        self, query: str, shard_config: Mapping[str, Any]
    ) -> List[ShardedQuery]:
        """
        Split ``query`` into one ``ShardedQuery`` per shard, bounding each to a
        slice of the source table as dictated by ``shard_config``.
        """
        raise NotImplementedError
