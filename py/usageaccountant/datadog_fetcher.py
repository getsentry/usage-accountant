import argparse
import json
import logging
import os
import urllib.parse
from typing import (
    Any,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    TextIO,
    TypedDict,
)
from urllib.request import Request, urlopen

from usageaccountant.accumulator import UsageAccumulator, UsageUnit

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
    unit: Sequence[DatadogResponseUnit]
    pointlist: Sequence[Sequence[float]]
    scope: str


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


class InvalidResponseError(Exception):
    def __init__(self, message: str, response: Mapping[Any, Any]) -> None:
        self.query = response.get("query")
        self.from_date = response.get("from_date")
        self.to_date = response.get("to_date")
        super().__init__(message)

    def __str__(self) -> str:
        return (
            f"{self.args[0]}; Query: {self.query}, "
            f"from_date: {self.from_date}, "
            f"to_date: {self.to_date}."
        )


class InvalidSeriesScopeError(Exception):
    def __init__(self, message: str, series: DatadogResponseSeries) -> None:
        self.scope = series["scope"]
        self.message = message

    def __str__(self) -> str:
        return f" {self.message}; Scope string: {self.scope}."


def is_valid_query_file(query_file: TextIO) -> bool:
    """
    Validates if the query file is not empty and
    that each dict contains a query and a unit
    """
    query_list = json.loads(query_file.read())
    assert isinstance(query_list, List)

    for query_dict in query_list:
        assert "query" in query_dict.keys()
        assert "unit" in query_dict.keys()

    return True


def is_valid_kafka_config_file(kafka_file: TextIO) -> bool:
    """
    Validates if the kafka config file is not empty and
    that it contains necessary fields
    """
    kafka_dict = json.loads(kafka_file.read())
    assert isinstance(kafka_dict, Mapping)

    assert "bootstrap_servers" in kafka_dict.keys()
    assert "config_params" in kafka_dict.keys()

    bootstrap_servers = kafka_dict["bootstrap_servers"]
    config_params = kafka_dict["config_params"]

    assert isinstance(bootstrap_servers, Sequence)
    assert isinstance(config_params, Mapping)

    return True


def is_valid_query(query: str) -> bool:
    """
    Validates if query string contains shared_resource_id
    and app_feature–parameters used in UsageAccountant.record().

    query: Datadog query
    """
    assert "shared_resource_id" in query
    assert "app_feature" in query

    return True


def is_valid_unit(unit: str) -> bool:
    """
    Validate if unit provided is supported by UsageAccumulator
    """
    try:
        UsageUnit(unit.lower())
    except ValueError:
        msg = f"Unsupported unit {unit} configured."
        raise ValueError(msg)

    return True


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


def parse_response_series(response: Mapping[Any, Any]) -> Any:
    """
    Extracts list of series objects from the API response.

    response: response received from API call
    """

    error_msg = response.get("error")
    if error_msg:
        raise InvalidResponseError(error_msg, response)

    if (
        response.get("series") is None
        or not isinstance(response.get("series"), Sequence)
        or len(response.get("series")) == 0
    ):
        msg = "No timeseries data found in response."
        raise InvalidResponseError(msg, response)

    return response.get("series")


def parse_response_scope(scope: str) -> Mapping[str, str]:
    """
    Parses scope string of the response into a dict
    """
    param_dict = {}
    parts = scope.split(",")
    for part in parts:
        sub_parts = part.split(":")
        assert len(sub_parts) == 2
        param_dict[sub_parts[0].strip()] = sub_parts[1].strip()

    return param_dict


def process_series_data(
    series_list: Sequence[DatadogResponseSeries], parsed_unit: UsageUnit
) -> Sequence[UsageAccumulatorRecord]:
    """
    Iterates through the series_list and
    returns a list of tuples.
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

    series_list: List of series dict extracted from API response
    parsed_unit: UsageUnit parsed from API response
    """
    record_list = []
    for series in series_list:
        scope = series.get("scope", "")
        scope_dict = parse_response_scope(scope)
        if (
            "shared_resource_id" not in scope_dict.keys()
            or "app_feature" not in scope_dict.keys()
        ):
            exception_msg = (
                "Required parameters, shared_resource_id and/or app_feature "
                "not found in series.scope of the series received."
            )
            raise InvalidSeriesScopeError(exception_msg, series)

        resource = scope_dict["shared_resource_id"]
        app_feature = scope_dict["app_feature"]
        for point in series.get("pointlist", []):
            # TODO remove this before prod
            # print(f"resource: {resource}, app_feature: {app_feature}, "
            #       f"amount: {int(point[1])}, usage_type: {parsed_unit}")

            record_list.append(
                UsageAccumulatorRecord(
                    resource_id=resource,
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
    Posts processed records to UsageAccumulator
    """
    for record in record_list:
        usage_accumulator.record(
            resource_id=record.resource_id,
            app_feature=record.app_feature,
            amount=record.amount,
            usage_type=record.usage_type,
        )


def main(
    query_file: TextIO,
    start_time: int,
    end_time: int,
    kafka_config_file: TextIO,
) -> None:
    """
    query_file: File with a list of dictionaries,
                each containing a Datadog query and unit
    start_time: Start of the time window in Unix epoch format
                with second-level precision
    end_time: End of the time window in Unix epoch format
              with second-level precision
    kafka_config_file: File with parameters to initialize
                       UsageAccumulator object
    """
    is_valid_kafka_config_file(kafka_config_file)
    kafka_config = json.loads(kafka_config_file.read())
    usage_accumulator = UsageAccumulator(kafka_config=kafka_config)

    is_valid_query_file(query_file)
    query_list = json.loads(query_file.read())
    record_list = []
    for query_dict in query_list:
        query, unit = query_dict.get("query"), query_dict.get("unit")

        if is_valid_query(query) and is_valid_unit(unit):
            response = query_datadog(query, start_time, end_time)
            series_list = parse_response_series(response)
            usage_unit = UsageUnit(unit.lower())
            record_list.extend(process_series_data(series_list, usage_unit))

    post_to_usage_accumulator(record_list, usage_accumulator)
    usage_accumulator.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetcher")

    parser.add_argument(
        "--query_file",
        type=argparse.FileType("r"),
        help="JSON file containing Datadog queries and units",
    )
    parser.add_argument(
        "--start_time", type=int, help="Start of the query time window"
    )
    parser.add_argument(
        "--end_time", type=int, help="End of the query time window"
    )
    parser.add_argument(
        "--kafka_config_file",
        type=argparse.FileType("r"),
        help="File containing kafka_config for initializing UsageAccumulator",
    )
    args = parser.parse_args()

    main(
        args.query_file, args.start_time, args.end_time, args.kafka_config_file
    )
