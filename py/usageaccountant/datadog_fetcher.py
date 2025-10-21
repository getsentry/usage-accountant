import argparse
import json
import logging
import os
import urllib.parse
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    TextIO,
    Tuple,
    TypedDict,
    cast,
)
from urllib.request import Request, urlopen

from usageaccountant.accumulator import (
    KafkaConfig,
    UsageAccumulator,
    UsageUnit,
)

headers = {
    "DD-APPLICATION-KEY": os.environ.get("DATADOG_APP_KEY", ""),
    "DD-API-KEY": os.environ.get("DATADOG_API_KEY", ""),
}

logger = logging.getLogger("fetcher")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class DatadogResponseUnit(TypedDict):
    plural: str
    name: str


class DatadogResponseSeries(TypedDict):
    unit: Optional[Tuple[DatadogResponseUnit, DatadogResponseUnit]]
    pointlist: Sequence[Sequence[float]]
    scope: str
    scope_dict: Mapping[str, str]


class DatadogResponse(TypedDict):
    error: Optional[str]
    query: str
    from_date: int
    to_date: int
    status: str
    series: Sequence[DatadogResponseSeries]


class UsageAccumulatorRecord(NamedTuple):
    resource_id: str
    app_feature: str
    amount: int
    usage_type: UsageUnit


def parse_and_assert_query_file(
    query_file: TextIO,
) -> Sequence[Mapping[str, str]]:
    """
    Validates if the query file is not empty and that
    each dict contains a query and shared_resource_id string.
    """
    query_list = json.loads(query_file.read())
    assert isinstance(query_list, List)

    for query_dict in query_list:
        assert "query" in query_dict
        assert "shared_resource_id" in query_dict

    return query_list


def parse_and_assert_kafka_config(kafka_config_file: TextIO) -> KafkaConfig:
    """
    Validates if the kafka config file contains
    bootstrap_servers and config_params.
    """
    kafka_config = json.loads(kafka_config_file.read())

    assert isinstance(kafka_config, Mapping)
    assert "bootstrap_servers" in kafka_config
    assert "config_params" in kafka_config

    bootstrap_servers = kafka_config["bootstrap_servers"]
    config_params = kafka_config["config_params"]

    if "sasl.password" in config_params:
        config_params["sasl.password"] = os.path.expandvars(
            config_params["sasl.password"]
        )

    assert isinstance(bootstrap_servers, Sequence)
    assert isinstance(config_params, Mapping)

    return KafkaConfig(
        bootstrap_servers=bootstrap_servers, config_params=config_params
    )


def assert_valid_query(query: str) -> None:
    """
    Validates if query string contains
    app_feature–parameter used in UsageAccumulator.record().

    We allow both `app_feature` and `feature` to account for
    the scenario where a getsentry pod is labeled with `app_feature`,
    but we *also* collect finer grained usage from within the container.
    In this situation, the application metrics cannot use `app_feature`
    as the tag value becomes `shared, issues`.

    query: Datadog query
    """
    assert "app_feature" in query or "feature" in query


def assert_valid_unit(unit: str) -> None:
    """
    Validates if unit provided is supported by UsageUnit enum.
    """
    UsageUnit(unit.lower())


def parse_and_assert_unit(series_list: Sequence[DatadogResponseSeries]) -> Any:
    """
    Extracts unit from a DatadogResponseSeries object.
    """
    # Return empty string if no series data is available
    if not series_list:
        return ""

    # assumes all the series have the same unit,
    # hence, using the first element
    assert "unit" in series_list[0]

    unit_list = series_list[0]["unit"]
    assert isinstance(unit_list, Sequence) and len(unit_list) > 0

    unit_dict = unit_list[0]
    assert isinstance(unit_dict, Mapping)
    assert "plural" in unit_dict

    return unit_dict["plural"]


def warn_different_configured_and_dd_unit(
    configured_unit: str, parsed_unit: str
) -> None:
    """
    Warns if the unit configured is different
    from the one received from Datadog.
    """
    if configured_unit.lower() != parsed_unit.lower():
        logger.warning(
            f"Configured unit {configured_unit} is different than"
            f" the one received from Datadog API, {parsed_unit}."
        )


def warn_multiple_units(series_list: Sequence[DatadogResponseSeries]) -> None:
    """
    Warns if multiple units are received from Datadog API.
    """
    # Return early if no series data is available
    if not series_list:
        return

    unit_list = series_list[0]["unit"]
    if unit_list and unit_list[1] is not None:
        logger.warning(f"Received multiple units from Datadog: {unit_list}.")


def query_datadog(query: str, start_time: int, end_time: int) -> Any:
    """
    Fetches timeseries data from Datadog API, returns response dict.

    query: Datadog query
    start_time: Start of the time window in Unix epoch format
                with second-level precision
    end_time: End of the time window in Unix epoch format
              with second-level precision
    """
    data = {"query": query, "from": start_time, "to": end_time}

    base_url = "https://api.datadoghq.com/api/v1/query"
    query_string = urllib.parse.urlencode(data)
    full_url = f"{base_url}?{query_string}"

    request = Request(url=full_url, headers=headers)
    response = urlopen(request).read()

    return json.loads(response)


def parse_and_assert_response_series(
    response: Mapping[Any, Any]
) -> Sequence[DatadogResponseSeries]:
    """
    Validates list of series objects in the API response.
    """
    assert "error" not in response
    assert "series" in response

    series_list = response.get("series")
    assert series_list is not None
    assert isinstance(series_list, Sequence)
    # Allow empty series_list - Datadog API can return no data

    casted_series_list = []
    for series in series_list:
        assert isinstance(series, Dict)
        assert "scope" in series
        assert "pointlist" in series
        casted_series = cast(DatadogResponseSeries, series)
        casted_series["scope_dict"] = parse_and_assert_response_scope(
            series["scope"]
        )
        casted_series_list.append(casted_series)

    return casted_series_list


def parse_and_assert_response_scope(scope: str) -> Mapping[str, str]:
    """
    Parses scope string of the response into a dict,
    and asserts presence of required key.
    """
    param_dict = {}
    parts = scope.split(",")
    for part in parts:
        sub_parts = part.split(":")
        assert len(sub_parts) == 2
        param_dict[sub_parts[0].strip()] = sub_parts[1].strip()

    assert "app_feature" in param_dict or "feature" in param_dict, (
        "Required parameters, `app_feature` or `feature` not found "
        "in series.scope of the series received."
    )

    return param_dict


def process_series_data(
    series_list: Sequence[DatadogResponseSeries],
    parsed_unit: UsageUnit,
    shared_resource_id: str,
) -> Sequence[UsageAccumulatorRecord]:
    """
    Iterates through the series_list and returns
    a list of UsageAccumulatorRecord.
    Sample response:

    {
       "status":"ok",
       "res_type":"time_series",
       "resp_version":1,
       "query":"avg: redis.mem.peak{app_feature: shared}
       by{shared_resource_id}.rollup(5)",
       "from_date":1721083881000,
       "to_date":1721083891000,
       "series":[
          {
             "unit":[
                {
                   "family":"bytes",
                   "id":2,
                   "name":"byte",
                   "short_name":"B",
                   "plural":"bytes",
                   "scale_factor":1.0
                },
                None
             ],
             "query_index":0,
             "aggr":"avg",
             "metric":"redis.mem.peak",
             "tag_set":["shared_resource_id: rc_long_redis"],
             "expression":"avg: redis.mem.peak{shared_resource_id:
             rc_long_redis,app_feature: shared}.rollup(5)",
             "scope":"app_feature: shared, shared_resource_id: rc_long_redis",
             "interval":5,
             "length":2,
             "start":1721083885000,
             "end":1721083894000,
             "pointlist":[
                [1721083885000.0, 2494386918.6666665],
                [1721083890000.0, 2494386918.6666665]
             ],
             "display_name":"redis.mem.peak",
             "attributes":{}
          }
       ],
       "values":[],
       "times":[],
       "message":"",
       "group_by":["shared_resource_id"]
    }

    series_list: List of DatadogResponseSeries extracted from API response
    parsed_unit: UsageUnit parsed from API response
    shared_resource_id: From query config file
    """
    record_list = []
    for series in series_list:
        if "app_feature" in series["scope_dict"]:
            app_feature = series["scope_dict"]["app_feature"]
        elif "feature" in series["scope_dict"]:
            app_feature = series["scope_dict"]["feature"]
        for point in series["pointlist"]:
            # skip records where there's no data recorded by Datadog
            if point[1] is not None:
                record_list.append(
                    UsageAccumulatorRecord(
                        resource_id=shared_resource_id,
                        app_feature=app_feature,
                        # point[0] is the timestamp
                        amount=int(point[1]),
                        usage_type=parsed_unit,
                    )
                )

    return record_list


def post_to_usage_accumulator(
    record_list: Sequence[UsageAccumulatorRecord],
    usage_accumulator: UsageAccumulator,
) -> None:
    """
    Posts UsageAccumulatorRecords to UsageAccumulator.
    """
    for record in record_list:
        usage_accumulator.record(
            resource_id=record.resource_id,
            app_feature=record.app_feature,
            amount=record.amount,
            usage_type=record.usage_type,
        )


def log_records(record_list: Sequence[UsageAccumulatorRecord]) -> None:
    """
    Logs UsageAccumulatorRecords.
    """
    for record in record_list:
        logger.info(
            f"resource_id: {record.resource_id}, "
            f"app_feature: {record.app_feature}, "
            f"amount: {record.amount}, "
            f"usage_type: {record.usage_type}."
        )


def main(
    query_file: TextIO,
    start_time: int,
    period_seconds: int,
    usage_accumulator: UsageAccumulator,
    dry_run: bool,
) -> None:
    """
    query_file: File with a list of dictionaries,
                each containing a Datadog query and unit
    start_time: Start of the time window in Unix epoch format
                with second-level precision
    period_seconds: Duration of the time window in seconds
    usage_accumulator: UsageAccumulator object
    """
    query_list = parse_and_assert_query_file(query_file)
    record_list: List[UsageAccumulatorRecord] = []
    for query_dict in query_list:
        query = query_dict["query"]
        configured_unit = query_dict.get("unit")
        shared_resource_id = query_dict["shared_resource_id"]

        assert_valid_query(query)

        response = query_datadog(
            query, start_time, (start_time + period_seconds)
        )
        series_list = parse_and_assert_response_series(response)

        if configured_unit:
            # need not raise an error if a unit is configured
            try:
                parsed_unit = parse_and_assert_unit(series_list)
            except AssertionError:
                parsed_unit = ""

            warn_different_configured_and_dd_unit(configured_unit, parsed_unit)
            unit = configured_unit
        else:
            parsed_unit = parse_and_assert_unit(series_list)
            unit = parsed_unit

        assert_valid_unit(unit)
        warn_multiple_units(series_list)
        usage_unit = UsageUnit(unit.lower())

        record_list.extend(
            process_series_data(series_list, usage_unit, shared_resource_id)
        )

    if dry_run:
        log_records(record_list)
    else:
        post_to_usage_accumulator(record_list, usage_accumulator)
        usage_accumulator.flush()
        usage_accumulator.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetcher")

    parser.add_argument(
        "--query_file",
        type=argparse.FileType("r"),
        help="JSON file containing Datadog queries and units",
    )
    parser.add_argument(
        "--start_time",
        type=int,
        help="Start of the query time window; "
        "defaults to now() quantized per period_seconds minus period_seconds",
        required=False,
    )
    parser.add_argument("--period_seconds", type=int, help="Period in seconds")
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

    if args.start_time is None:
        import time

        current_time = int(time.time())
        args.start_time = (
            current_time
            - (current_time % args.period_seconds)
            - args.period_seconds
        )

    kafka_config = parse_and_assert_kafka_config(args.kafka_config_file)

    main(
        args.query_file,
        args.start_time,
        args.period_seconds,
        UsageAccumulator(kafka_config=kafka_config),
        args.dry_run,
    )
