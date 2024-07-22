import json
import logging
import argparse
import urllib.parse

from urllib.request import Request, urlopen
from usageaccountant import UsageAccumulator, UsageUnit

headers = {
    "DD-APPLICATION-KEY": "3a26d82e978617862f8996a753d9bfcd1cefd297",
    "DD-API-KEY": "92c428d946b774db327ed1951ef00e68"
}

logger = logging.getLogger("fetcher")

# TODO remove this before prod
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


def is_valid_query(query: str) -> bool:
    """
    Validates if query string contains shared_resource_id
    and app_feature–parameters used in UsageAccountant.record().

    query (str): Datadog query
    """
    return "shared_resource_id" in query and "app_feature" in query


def get(query: str, start_time: int, end_time: int) -> dict:
    """
    Fetches timeseries data from Datadog API, returns response dict.

    query (str): Datadog query
    start_time (int): Start of the time window in Unix epoch format with second-level precision
    end_time (int): End of the time window in Unix epoch format with second-level precision
    """
    data = {
        "query": query,
        "from": start_time,
        "to": end_time
    }

    base_url = "https://api.datadoghq.com/api/v1/query"
    query_string = urllib.parse.urlencode(data)
    full_url = f"{base_url}?{query_string}"

    try:
        request = Request(url=full_url, headers=headers)
        # context is needed to handle
        # ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] on macOS
        # TODO remove next two lines and context parameter from the call to urlopen()
        import ssl
        context = ssl.SSLContext()
        response = urlopen(request, context=context).read()
        return json.loads(response)

    except Exception as e:
        raise e


def is_valid_response(response: dict) -> bool:
    """
    Validates response object for the following,
    1. presence of non-empty series list
    2. presence of a unit in the first element of series list
    3. presence of a unit that's supported by usageaccountant.UsageUnit

    response (dict): response received from API call
    """
    error_msg = response.get("error")
    if error_msg:
        raise Exception(format_exception_message(response, error_msg))

    if len(response.get("series", [])) == 0:
        msg = f"No timeseries data found in response."
        raise Exception(format_exception_message(response, msg))

    if len(response.get("series")[0].get("unit", [])) == 0:
        msg = f"No unit found in response."
        raise Exception(format_exception_message(response, msg))

    # assumes all series have one and the same unit
    response_unit = response.get("series")[0].get("unit")[0].get("plural", "")
    try:
        UsageUnit(response_unit.lower())
    except ValueError:
        msg = f"Unsupported unit {response_unit} received in response"
        raise ValueError(format_exception_message(response, msg))

    return True


def format_exception_message(response: dict, msg: str) -> str:
    """
    response: response received from API call
    msg: message to be logged
    """
    return f""" {msg}
    Query: {response.get("query")}, 
    from_date: {response.get("from_date")}, 
    to_date: {response.get("to_date")}"""


def parse_response_parameters(scope: str) -> dict:
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


def parse_and_post(response: dict, usage_accumulator: UsageAccumulator) -> None:
    """
    Parses the response object and posts the data points to UsageAccountant (hence, Kafka).
    Sample response:

    {
       "status":"ok",
       "res_type":"time_series",
       "resp_version":1,
       "query":"avg: redis.mem.peak{app_feature: shared}by{shared_resource_id}.rollup(5)",
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
             "expression":"avg: redis.mem.peak{shared_resource_id: rc_long_redis,app_feature: shared}.rollup(5)",
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

    response (dict): response received from API call
    usage_accumulated (obj): Object of UsageAccumulator class
    """
    # assumes all series have one and the same unit
    response_unit = response.get("series")[0].get("unit")[0].get("plural", "")
    parsed_unit = UsageUnit(response_unit.lower())

    for series in response.get("series"):
        scope = series.get("scope", "")
        scope_dict = parse_response_parameters(scope)
        if "shared_resource_id" not in scope_dict.keys() \
                or "app_feature" not in scope_dict.keys():
            exception_msg = format_exception_message(
                response,
                f"Required parameters, shared_resource_id and/or app_feature "
                f"not found in series.scope of the response received."
            )
            raise Exception(exception_msg)

        resource = scope_dict.get("shared_resource_id")
        app_feature = scope_dict.get("app_feature")
        for point in series.get("pointlist"):
            # TODO remove this before prod
            # print(f"resource: {resource}, app_feature: {app_feature}, amount: {int(point[1])}, usage_type: {parsed_unit}")
            usage_accumulator.record(
                resource_id=resource,
                app_feature=app_feature,
                # point[0] is the timestamp
                amount=int(point[1]),
                usage_type=parsed_unit
            )


def main(query, start_time, end_time, kafka_config) -> None:
    """
    query (str): Datadog query
    start_time (int): Start of the time window in Unix epoch format with second-level precision
    end_time (int): End of the time window in Unix epoch format with second-level precision
    kafka_config (dict): Parameters to initialize UsageAccumulator object
    """
    if not is_valid_query(query):
        raise Exception(f"Required parameter shared_resource_id and/or app_feature not found in the query: {query}.")

    response = get(query, start_time, end_time)

    if is_valid_response(response):
        ua = UsageAccumulator(kafka_config)
        parse_and_post(response, ua)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetcher")

    parser.add_argument("--query", type=str, help="Datadog query string")
    parser.add_argument("--start_time", type=int, help="Start of the query time window")
    parser.add_argument("--end_time", type=int, help="End of the query time window")
    parser.add_argument("--kafka_config", type=str, help="Stringified kafka config to initialize UsageAccumulator")

    args = parser.parse_args()
    main(
        args.query,
        args.start_time,
        args.end_time,
        json.loads(args.kafka_config)
    )
