import argparse
import json
import logging
import os
import urllib.parse
from typing import Any, Mapping, Optional, Sequence, TextIO, Tuple, TypedDict
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

# TODO remove handlers before prod
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
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


class InvalidResponseError(Exception):
    def __init__(self, message: str, response: Mapping[Any, Any]) -> None:
        self.query = response.get("query")
        self.from_date = response.get("from_date")
        self.to_date = response.get("to_date")
        self.message = message

    def str(self) -> str:
        return f""" {self.message}
                    Query: {self.query},
                    from_date: {self.from_date},
                    to_date: {self.to_date}."""


def is_valid_query(query: str) -> bool:
    """
    Validates if query string contains shared_resource_id
    and app_feature–parameters used in UsageAccountant.record().

    query: Datadog query
    """
    return "shared_resource_id" in query and "app_feature" in query


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
    # context is needed to handle
    # ssl.SSLCertVerificationError:
    # [SSL: CERTIFICATE_VERIFY_FAILED] on macOS
    # TODO remove next three lines and
    # context parameter from the call to urlopen()
    import ssl

    context = ssl.SSLContext()
    response = urlopen(request, context=context).read()
    return json.loads(response)


def parse_response_series_and_unit(
    response: Mapping[Any, Any]
) -> Tuple[Sequence[DatadogResponseSeries], UsageUnit]:
    """
    Extracts list of series objects and unit from the API response.

    response: response received from API call
    """

    error_msg = response.get("error")
    if error_msg:
        raise InvalidResponseError(error_msg, response)

    if response.get("series") is not None:
        series_list = response.get("series", [])
        if len(series_list) > 0:
            first_series = series_list[0]
            if first_series.get("unit") is not None:
                unit_list = first_series.get("unit")
                if isinstance(unit_list, Sequence) and len(unit_list) > 0:
                    first_unit = unit_list[0]
                    if first_unit.get("plural"):
                        # assumes all series have one and the same unit
                        response_unit = first_unit.get("plural")

    if len(series_list) == 0:
        msg = "No timeseries data found in response."
        raise InvalidResponseError(msg, response)

    if response_unit is None:
        msg = "No unit found in response."
        raise InvalidResponseError(msg, response)

    try:
        parsed_unit = UsageUnit(response_unit.lower())
    except ValueError:
        msg = f"Unsupported unit {response_unit} received in response."
        raise InvalidResponseError(msg, response)

    return series_list, parsed_unit


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


def parse_and_post(
    series_list: Sequence[DatadogResponseSeries],
    parsed_unit: UsageUnit,
    usage_accumulator: UsageAccumulator,
) -> None:
    """
    Iterates through the series_list and
    posts the data points to UsageAccountant (hence, Kafka).
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
    usage_accumulated: Object of UsageAccumulator class
    """
    for series in series_list:
        scope = series.get("scope", "")
        scope_dict = parse_response_scope(scope)
        if (
            "shared_resource_id" not in scope_dict.keys()
            or "app_feature" not in scope_dict.keys()
        ):
            exception_msg = (
                f"Required parameters, shared_resource_id and/or app_feature "
                f"not found in series.scope of the series {series} received."
            )
            raise Exception(exception_msg)

        resource = scope_dict["shared_resource_id"]
        app_feature = scope_dict["app_feature"]
        for point in series.get("pointlist", []):
            # TODO remove this before prod
            # print(f"resource: {resource}, app_feature: {app_feature}, "
            #       f"amount: {int(point[1])}, usage_type: {parsed_unit}")
            usage_accumulator.record(
                resource_id=resource,
                app_feature=app_feature,
                # point[0] is the timestamp
                amount=int(point[1]),
                usage_type=parsed_unit,
            )


def main(
    query_file: TextIO,
    start_time: int,
    end_time: int,
    kafka_config: KafkaConfig,
) -> None:
    """
    query: Datadog query
    start_time: Start of the time window in Unix epoch format
                with second-level precision
    end_time: End of the time window in Unix epoch format
              with second-level precision
    kafka_config: Parameters to initialize UsageAccumulator object
    """
    ua = UsageAccumulator(kafka_config=kafka_config)

    for line in query_file:
        query = line.rstrip()
        if not is_valid_query(query):
            raise Exception(
                f"Required parameter shared_resource_id and/or "
                f"app_feature not found in the query: {query}."
            )

        response = query_datadog(query, start_time, end_time)

        series_list, parsed_unit = parse_response_series_and_unit(response)
        parse_and_post(series_list, parsed_unit, ua)

    ua.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetcher")

    parser.add_argument(
        "--query_file",
        type=argparse.FileType("r"),
        help="File containing Datadog queries, one per line",
    )
    parser.add_argument(
        "--start_time", type=int, help="Start of the query time window"
    )
    parser.add_argument(
        "--end_time", type=int, help="End of the query time window"
    )
    parser.add_argument(
        "--kafka_config",
        type=str,
        help="Stringified kafka config to initialize UsageAccumulator",
    )
    args = parser.parse_args()

    main(
        args.query_file,
        args.start_time,
        args.end_time,
        json.loads(args.kafka_config),
    )
