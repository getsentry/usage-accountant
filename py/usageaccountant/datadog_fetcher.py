import argparse
import json
import logging
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, TypedDict
from urllib.request import Request, urlopen

from usageaccountant.accumulator import (
    KafkaConfig,
    UsageAccumulator,
    UsageUnit,
)

# TODO fetch these from env
headers = {"DD-APPLICATION-KEY": "", "DD-API-KEY": ""}

logger = logging.getLogger("fetcher")

# TODO remove this before prod
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)


class DatadogResponseUnit(TypedDict):
    plural: str
    name: str


class DatadogResponseSeries(DatadogResponseUnit):
    unit: List[DatadogResponseUnit]
    pointlist: List[List[float]]
    scope: str


class DatadogResponse(DatadogResponseSeries):
    error: Optional[str]
    query: str
    from_date: int
    to_date: int
    status: str
    series: List[DatadogResponseSeries]


def is_valid_query(query: str) -> bool:
    """
    Validates if query string contains shared_resource_id
    and app_feature–parameters used in UsageAccountant.record().

    query: Datadog query
    """
    return "shared_resource_id" in query and "app_feature" in query


def get(query: str, start_time: int, end_time: int) -> Any:
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

    try:
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

    except Exception as e:
        raise e


def parse_response_series_and_unit(
    response: DatadogResponse,
) -> Tuple[List[DatadogResponseSeries], UsageUnit]:
    """
    Extracts list of series objects and unit from the API response.

    response: response received from API call
    """

    error_msg = response.get("error")
    if error_msg:
        raise Exception(format_exception_message(response, error_msg))

    if response.get("series") is not None:
        series_list = response.get("series", [])
        if len(series_list) > 0:
            first_series = series_list[0]
            if first_series.get("unit") is not None:
                unit_list = first_series.get("unit")
                if isinstance(unit_list, List) and len(unit_list) > 0:
                    first_unit = unit_list[0]
                    if first_unit.get("plural"):
                        # assumes all series have one and the same unit
                        response_unit = first_unit.get("plural")

    if len(series_list) == 0:
        msg = "No timeseries data found in response."
        raise Exception(format_exception_message(response, msg))

    if response_unit is None:
        msg = "No unit found in response."
        raise Exception(format_exception_message(response, msg))

    try:
        parsed_unit = UsageUnit(response_unit.lower())
    except ValueError:
        msg = f"Unsupported unit {response_unit} received in response"
        raise ValueError(format_exception_message(response, msg))

    return series_list, parsed_unit


def format_exception_message(response: DatadogResponse, msg: str) -> str:
    """
    response: response received from API call
    msg: message to be logged
    """
    return f""" {msg}
    Query: {response.get("query")},
    from_date: {response.get("from_date")},
    to_date: {response.get("to_date")}"""


def parse_response_scope(scope: str) -> Dict[Any, Any]:
    """
    Parses scope string of the response into a dict
    """
    param_dict = {}
    parts = scope.split(",")
    for part in parts:
        if ":" in part:
            sub_parts = part.split(":")
            if len(sub_parts) == 2:
                param_dict[sub_parts[0].strip()] = sub_parts[1].strip()

    return param_dict


def parse_and_post(
    series_list: List[DatadogResponseSeries],
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

        resource = scope_dict.get("shared_resource_id", "")
        app_feature = scope_dict.get("app_feature", "")
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
    query: str, start_time: int, end_time: int, kafka_config: KafkaConfig
) -> None:
    """
    query: Datadog query
    start_time: Start of the time window in Unix epoch format
                with second-level precision
    end_time: End of the time window in Unix epoch format
              with second-level precision
    kafka_config: Parameters to initialize UsageAccumulator object
    """
    if not is_valid_query(query):
        raise Exception(
            f"Required parameter shared_resource_id and/or "
            f"app_feature not found in the query: {query}."
        )

    response = get(query, start_time, end_time)

    series_list, parsed_unit = parse_response_series_and_unit(response)
    ua = UsageAccumulator(kafka_config=kafka_config)
    parse_and_post(series_list, parsed_unit, ua)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetcher")

    parser.add_argument("--query", type=str, help="Datadog query string")
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
        args.query,
        args.start_time,
        args.end_time,
        json.loads(args.kafka_config),
    )
