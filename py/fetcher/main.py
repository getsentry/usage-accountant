import certifi
import logging

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.metrics_api import MetricsApi

from usageaccountant import UsageAccumulator, UsageUnit


logger = logging.getLogger("fetcher")

# TODO remove this before prod
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("debug.log"),
#         logging.StreamHandler()
#     ]
# )


def is_valid_query(query):
    """
    Validates if query string contains shared_resource_id
    and app_feature–parameters used in UsageAccountant.record().
    """
    return "shared_resource_id" in query and "app_feature" in query


def get(query, start_time, end_time):
    """
    Fetches timeseries data from Datadog API.

    query (str): Datadog query
    start_time (int): Start of the time window in Unix epoch format with second-level precision
    end_time (int): End of the time window in Unix epoch format with second-level precision
    returns model.metrics_query_response.MetricsQueryResponse
    """
    configuration = Configuration(ssl_ca_cert=certifi.where())
    api_client = ApiClient(configuration)
    api_instance = MetricsApi(api_client)
    response = api_instance.query_metrics(
        _from=start_time,
        to=end_time,
        query=query
    )

    return response


def is_valid_response(response):
    """
    Validates response object for the following,
    1. presence of non-empty series list
    2. presence of a unit in the first element of series list
    3. presence of a unit that's supported by usageaccountant.UsageUnit
    """
    error_msg = response.get("error")
    if error_msg:
        log_query_details(response, error_msg, logging.ERROR)
        return False

    if len(response.get("series", [])) == 0:
        msg = f"No timeseries data found in response."
        log_query_details(response, msg, logging.ERROR)
        return False

    if len(response.get("series")[0].get("unit", [])) == 0:
        msg = f"No unit found in response."
        log_query_details(response, msg, logging.ERROR)
        return False

    # assumes all series have one and the same unit
    response_unit = response.get("series")[0].get("unit")[0].get("plural", "")
    try:
        UsageUnit(response_unit.lower())
    except ValueError:
        msg = f"Unsupported unit {response_unit} received in response"
        log_query_details(response, msg, logging.ERROR)
        return False

    return True


def log_query_details(response, msg, level):
    logger.log(msg=f""" {msg}
    Query: {response.get("query")}, 
    from_date: {response.get("from_date")}, 
    to_date: {response.get("to_date")}""",
               level=level)


def parse_response_parameters(scope):
    parts = scope.split(",")
    param_dict = {}
    for part in parts:
        if ":" in part:
            sub_parts = part.split(":")
            if len(sub_parts) == 2:
                param_dict[sub_parts[0]] = sub_parts[1]

    return param_dict


def parse_and_post(response, usage_accumulator):
    """
    Parses the response object and posts the data points to Kafka.
    """
    # assumes all series have one and the same unit
    response_unit = response.get("series")[0].get("unit")[0].get("plural", "")
    parsed_unit = UsageUnit(response_unit.lower())

    for series in response.get("series"):
        scope = series.get("scope", "")
        scope_dict = parse_response_parameters(scope)
        if "shared_resource_id" not in scope_dict.keys() \
                or "app_feature" not in scope_dict.keys():
            log_query_details(
                response,
                f"Required parameters, shared_resource_id and/or app_feature "
                f"not found in series.scope of the response received.",
                logging.ERROR)

        resource = scope_dict.get("shared_resource_id")
        app_feature = scope_dict.get("app_feature")
        for point in series.pointlist:
            usage_accumulator.record(
                resource=resource,
                app_feature=app_feature,
                # point.value[0] is timestamp
                amount=int(point.value[1]),
                usage_type=parsed_unit
            )


def main(query, start_time, end_time, kafka_config):
    """
    query (str): Datadog query
    start_time (int): Start of the time window in Unix epoch format with second-level precision
    end_time (int): End of the time window in Unix epoch format with second-level precision
    """
    if not is_valid_query(query):
        logger.exception(f"Required parameter shared_resource_id and/or app_feature not found in the query: {query}.")
        raise

    response = get(query, start_time, end_time)

    if is_valid_response(response):
        ua = UsageAccumulator(kafka_config)
        parse_and_post(response, ua)


if __name__ == "__main__":
    main(
        "avg:redis.mem.peak{app_feature:shared} by {shared_resource_id}.rollup(5)",
        1721083881,
        1721083891,
        kafka_config={}
    )
