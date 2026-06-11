import argparse
import logging
from typing import Any, Iterable, List, Mapping, Sequence, TextIO

from google.cloud import bigquery

from usageaccountant.accumulator import UsageAccumulator, UsageUnit
from usageaccountant.fetcher_utils import (
    assert_valid_unit,
    log_records,
    parse_and_assert_kafka_config,
    post_to_usage_accumulator,
    UsageAccumulatorRecord,
)

logger = logging.getLogger("bigquery_fetcher")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

APP_FEATURE_COLUMN = "app_feature"
AMOUNT_COLUMN = "amount"
# Optional column: Unix epoch seconds for the day the usage applies to
# (e.g. the Storage Insights inventory date). When absent, the accumulator
# stamps the record with the run time.
TIMESTAMP_COLUMN = "timestamp"


def parse_and_assert_query_file(
    query_file: TextIO,
) -> Sequence[Mapping[str, str]]:
    """
    Validates that the query file is a non-empty list and that each entry
    contains a ``query``, a ``shared_resource_id`` and a ``unit``.
    """
    import json

    query_list = json.loads(query_file.read())
    assert isinstance(query_list, list)
    assert query_list, "query file must contain at least one query"

    for query_dict in query_list:
        assert "query" in query_dict
        assert "shared_resource_id" in query_dict
        assert "unit" in query_dict

    return query_list


def process_rows(
    rows: Iterable[Mapping[str, Any]], unit: UsageUnit, shared_resource_id: str
) -> Sequence[UsageAccumulatorRecord]:
    """
    Maps the rows returned by a rollup query into UsageAccumulatorRecords.

    Each row must expose an ``app_feature`` and an ``amount`` column, and may
    optionally expose a ``timestamp`` column (Unix epoch seconds).
    """
    record_list = []
    for row in rows:
        app_feature = row[APP_FEATURE_COLUMN]
        amount = row[AMOUNT_COLUMN]
        # Skip rows with no usage recorded for this feature/day.
        if app_feature is None or amount is None:
            continue

        timestamp = row.get(TIMESTAMP_COLUMN, None)

        record_list.append(
            UsageAccumulatorRecord(
                resource_id=shared_resource_id,
                app_feature=app_feature,
                amount=int(amount),
                usage_type=unit,
                timestamp=None if timestamp is None else int(timestamp),
            )
        )

    return record_list


def main(
    query_file: TextIO,
    usage_accumulator: UsageAccumulator,
    bq_client: bigquery.Client,
    dry_run: bool,
) -> None:
    """
    query_file: File with a list of dictionaries, each containing a BigQuery
                ``query``, a ``shared_resource_id`` and a ``unit``.
    usage_accumulator: UsageAccumulator object.
    query_runner: Callable that runs a query and yields result rows.
    dry_run: When True, log the records instead of producing them to Kafka.
    """
    query_list = parse_and_assert_query_file(query_file)
    record_list: List[UsageAccumulatorRecord] = []
    for query_dict in query_list:
        query = query_dict["query"]
        unit = query_dict["unit"]
        shared_resource_id = query_dict["shared_resource_id"]

        assert_valid_unit(unit)
        usage_unit = UsageUnit(unit.lower())

        rows = bq_client.query(query).result()
        record_list.extend(process_rows(rows, usage_unit, shared_resource_id))

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
