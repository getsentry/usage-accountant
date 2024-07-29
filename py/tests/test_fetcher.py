import copy
import os
import unittest
from io import StringIO

from typing_extensions import Mapping, Sequence, cast

from tests.test_data import datadog_response
from usageaccountant import accumulator
from usageaccountant import datadog_fetcher as ddf


def read_data_file(filename: str) -> str:
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    query_file_path = os.path.join(tests_dir, "test_data", filename)
    with open(query_file_path, "r") as file:
        return file.read()


class TestDatadogFetcher(unittest.TestCase):
    def setUp(self) -> None:
        self.good_response = copy.deepcopy(datadog_response.good_response)
        self.bad_response = copy.deepcopy(datadog_response.bad_response)
        self.processed_response = copy.deepcopy(
            datadog_response.processed_response
        )

    def test_parse_and_assert_query_file(self) -> None:
        query_io = StringIO(read_data_file("query.json"))
        assert ddf.parse_and_assert_query_file(query_io)

    def test_parse_and_assert_query_file_no_query(self) -> None:
        query_io = StringIO('[{"unit": "bytes"}]')
        self.assertRaises(
            AssertionError, ddf.parse_and_assert_query_file, query_io
        )

    def test_parse_and_assert_kafka_config(self) -> None:
        kafka_io = StringIO(read_data_file("kafka.json"))
        ddf.parse_and_assert_kafka_config(kafka_io)

    def test_parse_and_assert_kafka_config_no_config_params(self) -> None:
        kafka_io = StringIO(
            '{"bootstrap_servers": ["kafka.service.host:1234"]}'
        )
        self.assertRaises(
            AssertionError, ddf.parse_and_assert_kafka_config, kafka_io
        )

    def test_parse_and_assert_unit(self) -> None:
        parsed_unit = ddf.parse_and_assert_unit(
            cast(
                Sequence[ddf.DatadogResponseSeries],
                self.good_response.get("series"),
            )
        )
        assert parsed_unit == "bytes"

    def test_parse_and_assert_unit_no_unit(self) -> None:
        series = self.good_response["series"]
        series[0].pop("unit")  # type: ignore
        self.assertRaises(
            AssertionError,
            ddf.parse_and_assert_unit,
            cast(ddf.DatadogResponseSeries, series),
        )

    def test_warn_multiple_units(self) -> None:
        series_list = self.good_response["series"]
        series_list[0]["unit"][1] = {"dummy": "unit"}  # type: ignore
        with self.assertLogs("fetcher") as cm:
            ddf.warn_multiple_units(
                cast(Sequence[ddf.DatadogResponseSeries], series_list)
            )
            assert len(cm.output) == 1

    def test_parse_and_assert_response_series(self) -> None:
        series_list = ddf.parse_and_assert_response_series(self.good_response)
        assert len(series_list) == 1

    def test_parse_and_assert_response_series_error(self) -> None:
        self.assertRaises(
            AssertionError,
            ddf.parse_and_assert_response_series,
            self.bad_response,
        )

    def test_parse_and_assert_response_scope(self) -> None:
        sample_scope = self.good_response["series"][0].get(  # type: ignore
            "scope"
        )

        self.assertDictEqual(
            ddf.parse_and_assert_response_scope(sample_scope),
            cast(
                Mapping[str, str],
                self.processed_response["series"][0]["scope_dict"],
            ),
        )

    def test_parse_and_assert_response_scope_no_app_feature(self) -> None:
        self.assertRaises(
            AssertionError,
            ddf.parse_and_assert_response_scope,
            '"shared_resource_id": "rc_long_redis"',
        )

    def test_process_series_data(self) -> None:
        expected_record_list = [
            ddf.UsageAccumulatorRecord(
                resource_id="rc_long_redis",
                app_feature="shared",
                amount=2,
                usage_type=accumulator.UsageUnit.BYTES,
            ),
            ddf.UsageAccumulatorRecord(
                resource_id="rc_long_redis",
                app_feature="shared",
                amount=3,
                usage_type=accumulator.UsageUnit.BYTES,
            ),
        ]

        self.assertEqual(
            ddf.process_series_data(
                cast(
                    Sequence[ddf.DatadogResponseSeries],
                    self.processed_response["series"],
                ),
                accumulator.UsageUnit.BYTES,
            ),
            expected_record_list,
        )
