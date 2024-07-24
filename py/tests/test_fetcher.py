import copy
import unittest
from typing import List
from unittest.mock import call, patch

from typing_extensions import cast

from tests.test_data import datadog_response
from usageaccountant import accumulator
from usageaccountant import datadog_fetcher as ddf


class TestDatadogFetcher(unittest.TestCase):
    def setUp(self) -> None:
        self.good_response = copy.deepcopy(datadog_response.good_response)
        self.bad_response = copy.deepcopy(datadog_response.bad_response)

    def test_parse_response_scope(self) -> None:
        expected_dict = {
            "app_feature": "shared",
            "shared_resource_id": "rc_long_redis",
        }
        sample_scope = self.good_response.get("series")[0].get(  # type: ignore
            "scope"
        )
        returned_dict = ddf.parse_response_scope(sample_scope)
        self.assertDictEqual(expected_dict, returned_dict)

    def test_parse_response_series_and_unit(self) -> None:
        series_list, parsed_unit = ddf.parse_response_series_and_unit(
            cast(ddf.DatadogResponse, self.good_response)
        )
        assert len(series_list) == 1
        assert isinstance(parsed_unit, accumulator.UsageUnit)

    def test_parse_response_series_and_unit_error(self) -> None:
        self.assertRaises(
            Exception, ddf.parse_response_series_and_unit, self.bad_response
        )

    def test_parse_response_series_and_unit_no_unit(self) -> None:
        self.good_response.get("series")[0].pop("unit")  # type: ignore
        self.assertRaises(
            Exception, ddf.parse_response_series_and_unit, self.good_response
        )

    def test_ua_is_called(self) -> None:
        kafka_config = accumulator.KafkaConfig("", {})
        ua = accumulator.UsageAccumulator(kafka_config=kafka_config)
        series_list = self.good_response.get("series")
        parsed_unit = accumulator.UsageUnit.BYTES

        calls = [
            call(
                resource_id="rc_long_redis",
                app_feature="shared",
                amount=2,
                usage_type=accumulator.UsageUnit("bytes"),
            ),
            call(
                resource_id="rc_long_redis",
                app_feature="shared",
                amount=3,
                usage_type=accumulator.UsageUnit("bytes"),
            ),
        ]

        with patch.object(ua, "record") as mocked_record:
            ddf.parse_and_post(
                cast(List[ddf.DatadogResponseSeries], series_list),
                parsed_unit,
                ua,
            )

            mocked_record.assert_has_calls(calls, any_order=True)


if __name__ == "__main__":
    unittest.main()
