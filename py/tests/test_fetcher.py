import copy
import unittest

from unittest.mock import MagicMock, call
from fetcher import datadog
from usageaccountant import accumulator
from tests.test_data import datadog_response


class TestDatadogFetcher(unittest.TestCase):
    def setUp(self) -> None:
        self.good_response = copy.deepcopy(datadog_response.good_response)
        self.bad_response = copy.deepcopy(datadog_response.bad_response)

    def test_parse_response_parameters(self) -> None:
        expected_dict = {
            "app_feature": "shared",
            "shared_resource_id": "rc_long_redis"
        }
        sample_scope = self.good_response.get("series")[0].get("scope")
        returned_dict = datadog.parse_response_parameters(sample_scope)
        self.assertDictEqual(expected_dict, returned_dict)

    def test_is_valid_response(self) -> None:
        self.assertTrue(datadog.is_valid_response(self.good_response))

    def test_is_valid_response_error(self) -> None:
        self.assertRaises(Exception, datadog.is_valid_response, self.bad_response)

    def test_is_valid_response_no_unit(self) -> None:
        self.good_response.get("series")[0].pop("unit")
        self.assertRaises(Exception, datadog.is_valid_response, self.good_response)

    def test_ua_is_called(self) -> None:
        kafka_config = accumulator.KafkaConfig("", {})
        ua = accumulator.UsageAccumulator(kafka_config=kafka_config)
        ua.record = MagicMock()
        datadog.parse_and_post(self.good_response, ua)
        calls = [
            call(
                resource_id='rc_long_redis', app_feature='shared', amount=2, usage_type=accumulator.UsageUnit('bytes')),
            call(
                resource_id='rc_long_redis', app_feature='shared', amount=3, usage_type=accumulator.UsageUnit('bytes'))
        ]
        ua.record.assert_has_calls(calls, any_order=True)


if __name__ == '__main__':
    unittest.main()
