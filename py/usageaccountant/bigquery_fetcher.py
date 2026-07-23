import argparse
import logging
from datetime import date, datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    TextIO,
    Tuple,
)

from google.cloud import bigquery

from usageaccountant.accumulator import UsageAccumulator, UsageUnit
from usageaccountant.bigtable_sharding import BigtableQuerySharder
from usageaccountant.fetcher_utils import (
    UsageAccumulatorRecord,
    assert_valid_unit,
    log_records,
    parse_and_assert_kafka_config,
    post_to_usage_accumulator,
)
from usageaccountant.query_sharding import QuerySharder

logger = logging.getLogger("bigquery_fetcher")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

APP_FEATURE_COLUMN = "app_feature"
AMOUNT_COLUMN = "amount"
# Optional column identifying the time the usage applies to (e.g. the Storage
# Insights inventory date). Prefer emitting Unix epoch seconds as an ``INT64``
# (e.g. ``UNIX_SECONDS(...)``); ``DATE`` and ``TIMESTAMP`` columns are also
# accepted and normalized (see ``normalize_timestamp``). When absent, the
# accumulator stamps the record with the run time.
TIMESTAMP_COLUMN = "timestamp"

# Maps a query entry's shard-config key to the QuerySharder that handles it.
# A query carrying one of these keys is run per shard (see
# ``run_sharded_query``); to add another backend, register its sharder here.
SHARDERS: Mapping[str, QuerySharder] = {
    "bigtable_shard": BigtableQuerySharder()
}


def select_sharder(
    query_dict: Mapping[str, Any], sharders: Mapping[str, QuerySharder]
) -> Optional[Tuple[Mapping[str, Any], QuerySharder]]:
    """
    Returns the ``(shard_config, sharder)`` for whichever registered
    shard-config key is present in ``query_dict``, or ``None`` for an unsharded
    query.
    """
    for key, sharder in sharders.items():
        shard_config = query_dict.get(key)
        if shard_config is not None:
            return shard_config, sharder
    return None


def normalize_timestamp(value: Any) -> Optional[int]:
    """
    Normalizes a BigQuery ``timestamp`` column value to integer Unix epoch
    seconds (or ``None`` when the value is SQL ``NULL``).

    The expected and preferred type is an ``INT64`` of Unix epoch seconds
    (e.g. from ``UNIX_SECONDS(...)``), which the BigQuery client already
    returns as a Python ``int`` -- so no conversion is needed. For robustness
    we additionally accept:
      * ``FLOAT64`` -> truncated to ``int``
      * ``TIMESTAMP`` -> ``datetime.datetime`` (tz-aware) via ``.timestamp()``
      * ``DATE`` -> ``datetime.date``, interpreted as midnight UTC

    Any other type (including ``bool``, which is a subclass of ``int``) raises
    ``TypeError`` rather than being silently coerced to a nonsensical epoch.
    """
    if value is None:
        return None
    # bool is a subclass of int; reject explicitly so True/False do not become
    # epoch 1/0.
    if isinstance(value, bool):
        raise TypeError(f"invalid timestamp value: {value!r}")
    # datetime is a subclass of date, so check it first.
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, date):
        return int(
            datetime(
                value.year, value.month, value.day, tzinfo=timezone.utc
            ).timestamp()
        )
    if isinstance(value, (int, float)):
        return int(value)
    raise TypeError(
        f"unsupported timestamp type: {type(value).__name__} ({value!r})"
    )


def parse_and_assert_query_file(
    query_file: TextIO, sharders: Mapping[str, QuerySharder] = SHARDERS
) -> Sequence[Mapping[str, Any]]:
    """
    Validates that the query file is a non-empty list and that each entry
    contains a ``query``, a ``shared_resource_id`` and a ``unit``.

    An entry may optionally carry a shard-config key (e.g. ``bigtable_shard``)
    to run the query once per shard (see ``run_sharded_query``). When present
    its config is validated by the corresponding ``QuerySharder``.
    """
    import json

    query_list = json.loads(query_file.read())
    assert isinstance(query_list, list)
    assert query_list, "query file must contain at least one query"

    for query_dict in query_list:
        assert "query" in query_dict
        assert "shared_resource_id" in query_dict
        assert "unit" in query_dict

        selected = select_sharder(query_dict, sharders)
        if selected is not None:
            shard_config, sharder = selected
            sharder.validate_config(shard_config, query_dict["query"])

    return query_list


def process_rows(
    rows: Iterable[Mapping[str, Any]], unit: UsageUnit, shared_resource_id: str
) -> Sequence[UsageAccumulatorRecord]:
    """
    Maps the rows returned by a rollup query into UsageAccumulatorRecords.

    Each row must expose an ``app_feature`` and an ``amount`` column, and may
    optionally expose a ``timestamp`` column (see ``normalize_timestamp`` for
    the accepted types).
    """
    record_list = []
    for row in rows:
        app_feature = row[APP_FEATURE_COLUMN]
        amount = row[AMOUNT_COLUMN]
        # Skip rows with no usage recorded for this feature/day.
        if app_feature is None or amount is None:
            continue

        timestamp = normalize_timestamp(row.get(TIMESTAMP_COLUMN, None))

        record_list.append(
            UsageAccumulatorRecord(
                resource_id=shared_resource_id,
                app_feature=app_feature,
                amount=int(amount),
                usage_type=unit,
                timestamp=timestamp,
            )
        )

    return record_list


def run_sharded_query(
    bq_client: bigquery.Client,
    query: str,
    shard_config: Mapping[str, Any],
    sharder: QuerySharder,
) -> Iterator[Mapping[str, Any]]:
    """
    Runs ``query`` once per shard and yields every row.

    The query is broken into shards by ``sharder`` (see ``QuerySharder``).
    Each shard is a self-contained query bounded to a slice of the source
    table. This is useful for evading BigQuery's 6h timeout for long-running
    queries.

    Partial per-shard results are combined by the caller.
    """
    shard_queries = sharder.build_shard_queries(query, shard_config)
    logger.info("Query split into %d shard(s)", len(shard_queries))

    for index, shard in enumerate(shard_queries):
        logger.info(
            "Running shard %d/%d %s",
            index + 1,
            len(shard_queries),
            shard.description,
        )
        job_config = bigquery.QueryJobConfig(query_parameters=shard.parameters)
        query_job = bq_client.query(shard.sql, job_config=job_config)
        yield from query_job.result()


def aggregate_records(
    records: Iterable[UsageAccumulatorRecord],
) -> List[UsageAccumulatorRecord]:
    """
    Sums record amounts sharing the same
    ``(resource_id, app_feature, usage_type, timestamp)`` key.
    """
    totals: Dict[Tuple[str, str, UsageUnit, Optional[int]], int] = {}
    for record in records:
        key = (
            record.resource_id,
            record.app_feature,
            record.usage_type,
            record.timestamp,
        )
        totals[key] = totals.get(key, 0) + record.amount

    return [
        UsageAccumulatorRecord(
            resource_id=resource_id,
            app_feature=app_feature,
            amount=amount,
            usage_type=usage_type,
            timestamp=timestamp,
        )
        for (
            resource_id,
            app_feature,
            usage_type,
            timestamp,
        ), amount in totals.items()
    ]


def main(
    query_file: TextIO,
    usage_accumulator: UsageAccumulator,
    bq_client: bigquery.Client,
    dry_run: bool,
    sharders: Optional[Mapping[str, QuerySharder]] = None,
) -> None:
    """
    query_file: File with a list of dictionaries, each containing a BigQuery
                ``query``, a ``shared_resource_id`` and a ``unit``. An entry
                may also carry a shard-config key (e.g. ``bigtable_shard``) to
                break the query into shards.
    usage_accumulator: UsageAccumulator object.
    bq_client: BigQuery client used to run the queries.
    dry_run: When True, log the records instead of producing them to Kafka.
    sharders: Registry mapping shard-config keys to their ``QuerySharder``;
              defaults to ``SHARDERS``. Injectable for testing.
    """
    if sharders is None:
        sharders = SHARDERS

    query_list = parse_and_assert_query_file(query_file, sharders)
    record_list: List[UsageAccumulatorRecord] = []
    for query_dict in query_list:
        query = query_dict["query"]
        unit = query_dict["unit"]
        shared_resource_id = query_dict["shared_resource_id"]
        selected = select_sharder(query_dict, sharders)

        assert_valid_unit(unit)
        usage_unit = UsageUnit(unit.lower())

        if selected is not None:
            shard_config, sharder = selected
            rows: Iterable[Mapping[str, Any]] = run_sharded_query(
                bq_client, query, shard_config, sharder
            )
            records = process_rows(rows, usage_unit, shared_resource_id)
            record_list.extend(aggregate_records(records))
        else:
            rows = bq_client.query(query).result()
            record_list.extend(
                process_rows(rows, usage_unit, shared_resource_id)
            )

    if dry_run:
        log_records(logger, record_list)
    else:
        post_to_usage_accumulator(record_list, usage_accumulator)
        usage_accumulator.flush()
        usage_accumulator.close()


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="bigquery_fetcher")
    parser.add_argument(
        "--query_file",
        type=argparse.FileType("r"),
        help="JSON file containing BigQuery rollup queries, "
        "shared_resource_ids and units",
    )
    parser.add_argument(
        "--kafka_config_file",
        type=argparse.FileType("r"),
        help="File containing kafka_config for initializing UsageAccumulator",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Log data instead of sending it to Kafka",
    )
    args = parser.parse_args()

    kafka_config = parse_and_assert_kafka_config(args.kafka_config_file)

    bq_client = bigquery.Client()

    main(
        args.query_file,
        UsageAccumulator(kafka_config=kafka_config),
        bq_client,
        args.dry_run,
    )
