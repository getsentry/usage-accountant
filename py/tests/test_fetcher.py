import copy
import os
import unittest
from email.message import Message
from io import StringIO
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Partition, Topic
from arroyo.utils.clock import MockedClock
from typing_extensions import Mapping, Sequence, cast

from tests.test_data import datadog_response
from usageaccountant import accumulator
from usageaccountant import datadog_fetcher as ddf


class TestDatadogFetcher(unittest.TestCase):
    query_str = (
        '[{"query": "sum:zookeeper.bytes_received{*} '
        'by {app_feature,shared_resource_id}","unit": "bytes",'
        '"shared_resource_id": "rc_long_redis"}]'
    )
    kafka_config_str = (
        '{"bootstrap_servers": ["kafka.service.host:1234"],'
        '"config_params": '
        '{"compression.type": "lz4",'
        '"message.max.bytes": 10}}'
    )

    def setUp(self) -> None:
        self.good_response = copy.deepcopy(datadog_response.good_response)
        self.good_response_feature_tag = copy.deepcopy(
            datadog_response.good_response_feature_tag
        )
        self.bad_response = copy.deepcopy(datadog_response.bad_response)
        self.processed_response = copy.deepcopy(
            datadog_response.processed_response
        )
        self.processed_response_with_none = copy.deepcopy(
            datadog_response.processed_response_with_none
        )

        storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
        self.broker = LocalBroker(storage, MockedClock())
        self.topic = Topic("test_dd_fetcher")
        self.broker.create_topic(self.topic, 1)

        producer = self.broker.get_producer()
        self.usage_accumulator = accumulator.UsageAccumulator(
            1, topic_name="test_dd_fetcher", producer=producer
        )

    def test_parse_and_assert_query_file(self) -> None:
        query_io = StringIO(self.query_str)
        assert ddf.parse_and_assert_query_file(query_io)

    def test_parse_and_assert_query_file_no_query(self) -> None:
        query_io = StringIO('[{"unit": "bytes"}]')
        self.assertRaises(
            AssertionError, ddf.parse_and_assert_query_file, query_io
        )

    def test_parse_and_assert_kafka_config(self) -> None:
        kafka_io = StringIO(self.kafka_config_str)
        ddf.parse_and_assert_kafka_config(kafka_io)

    def test_parse_and_assert_kafka_config_no_config_params(self) -> None:
        kafka_io = StringIO(
            '{"bootstrap_servers": ["kafka.service.host:1234"]}'
        )
        self.assertRaises(
            AssertionError, ddf.parse_and_assert_kafka_config, kafka_io
        )

    def test_parse_and_assert_expandvars(self) -> None:
        os.environ["SASL_PASSWORD"] = "supersecret"

        expected_kafka_config = accumulator.KafkaConfig(
            bootstrap_servers=["kafka.service.host:1234"],
            config_params={
                "sasl.username": "alice",
                "sasl.password": "supersecret",
            },
        )

        kafka_io = StringIO(
            '{"bootstrap_servers": ["kafka.service.host:1234"],'
            '"config_params": '
            '{"sasl.username": "alice",'
            '"sasl.password": "${SASL_PASSWORD}"}}'
        )

        self.assertEqual(
            ddf.parse_and_assert_kafka_config(kafka_io), expected_kafka_config
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

    def test_parse_and_assert_unit_empty_series(self) -> None:
        """
        Test that parse_and_assert_unit handles empty series list gracefully.
        """

        result = ddf.parse_and_assert_unit([])
        self.assertEqual(result, "")

    def test_warn_multiple_units(self) -> None:
        series_list = self.good_response["series"]
        series_list[0]["unit"][1] = {"dummy": "unit"}  # type: ignore
        with self.assertLogs("fetcher") as cm:
            ddf.warn_multiple_units(
                cast(Sequence[ddf.DatadogResponseSeries], series_list)
            )
            assert len(cm.output) == 1

    def test_warn_multiple_units_empty_series(self) -> None:
        """
        Test that warn_multiple_units handles empty series list gracefully.
        """

        # This should not raise any exception or log any warnings
        ddf.warn_multiple_units([])

    def test_parse_and_assert_response_series(self) -> None:
        series_list = ddf.parse_and_assert_response_series(self.good_response)
        assert len(series_list) == 1

    def test_parse_and_assert_response_series_feature_tag(self) -> None:
        series_list = ddf.parse_and_assert_response_series(
            self.good_response_feature_tag
        )
        assert len(series_list) == 1

    def test_parse_and_assert_response_series_error(self) -> None:
        self.assertRaises(
            AssertionError,
            ddf.parse_and_assert_response_series,
            self.bad_response,
        )

    def test_parse_and_assert_response_series_empty(self) -> None:
        """
        Test that parse_and_assert_response_series handles empty series
        gracefully.
        """

        empty_response = {"status": "ok", "series": []}
        series_list = ddf.parse_and_assert_response_series(empty_response)
        self.assertEqual(len(series_list), 0)

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
                "rc_long_redis",
            ),
            expected_record_list,
        )

    def test_process_series_data_with_none(self) -> None:
        expected_record_list = [
            ddf.UsageAccumulatorRecord(
                resource_id="rc_long_redis",
                app_feature="shared",
                amount=2,
                usage_type=accumulator.UsageUnit.BYTES,
            )
        ]

        self.assertEqual(
            ddf.process_series_data(
                cast(
                    Sequence[ddf.DatadogResponseSeries],
                    self.processed_response_with_none["series"],
                ),
                accumulator.UsageUnit.BYTES,
                "rc_long_redis",
            ),
            expected_record_list,
        )

    def test_process_series_data_feature_tag(self) -> None:
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
        parsed = ddf.parse_and_assert_response_series(
            self.good_response_feature_tag
        )
        self.assertEqual(
            ddf.process_series_data(
                parsed, accumulator.UsageUnit.BYTES, "rc_long_redis"
            ),
            expected_record_list,
        )

    @patch("usageaccountant.datadog_fetcher.query_datadog")
    @patch("time.time")
    def test_main(self, mock_time: Mock, mock_query_dd: Mock) -> None:
        mock_query_dd.return_value = self.good_response
        mock_time.return_value = 1

        ddf.main(
            query_file=StringIO(self.query_str),
            start_time=1,
            period_seconds=2,
            usage_accumulator=self.usage_accumulator,
            dry_run=False,
        )

        msg1 = self.broker.consume(Partition(self.topic, 0), 0)
        assert msg1 is not None

        msg2 = self.broker.consume(Partition(self.topic, 0), 1)
        assert msg2 is None

    @patch("usageaccountant.datadog_fetcher.query_datadog")
    @patch("time.time")
    def test_main_series_missing_unit_key(
        self, mock_time: Mock, mock_query_dd: Mock
    ) -> None:
        """
        Datadog can omit the "unit" key from a series entirely. With a
        configured unit, main() should still record the data.
        """
        self.good_response["series"][0].pop("unit")  # type: ignore
        mock_query_dd.return_value = self.good_response
        mock_time.return_value = 1

        ddf.main(
            query_file=StringIO(self.query_str),
            start_time=1,
            period_seconds=2,
            usage_accumulator=self.usage_accumulator,
            dry_run=False,
        )

        msg1 = self.broker.consume(Partition(self.topic, 0), 0)
        assert msg1 is not None

        msg2 = self.broker.consume(Partition(self.topic, 0), 1)
        assert msg2 is None

    @patch("usageaccountant.datadog_fetcher.query_datadog")
    @patch("time.time")
    def test_main_no_unit(self, mock_time: Mock, mock_query_dd: Mock) -> None:
        mock_query_dd.return_value = self.good_response
        mock_time.return_value = 1

        ddf.main(
            query_file=StringIO(
                '[{"query": "avg:redis.mem.peak{app_feature:shared} '
                'by {shared_resource_id}.rollup(5)", '
                '"shared_resource_id": "rc_long_redis"}]'
            ),
            start_time=1,
            period_seconds=2,
            usage_accumulator=self.usage_accumulator,
            dry_run=False,
        )

        msg1 = self.broker.consume(Partition(self.topic, 0), 0)
        assert msg1 is not None

        msg2 = self.broker.consume(Partition(self.topic, 0), 1)
        assert msg2 is None


def _http_error(code: int) -> HTTPError:
    return HTTPError(
        "https://api.datadoghq.com", code, "error", Message(), None
    )


def _ok_response() -> Mock:
    response = Mock()
    response.read.return_value = b'{"series": []}'
    return response


@patch("usageaccountant.datadog_fetcher.time.sleep")
@patch("usageaccountant.datadog_fetcher.urlopen")
class TestQueryDatadogRetry(unittest.TestCase):
    def test_retries_server_error(
        self, mock_urlopen: Mock, mock_sleep: Mock
    ) -> None:
        mock_urlopen.side_effect = [_http_error(503), _ok_response()]

        assert ddf.query_datadog("q", 0, 1) == {"series": []}
        assert mock_urlopen.call_count == 2
        assert mock_sleep.call_count == 1

    def test_retries_network_error(
        self, mock_urlopen: Mock, mock_sleep: Mock
    ) -> None:
        mock_urlopen.side_effect = [
            URLError("timed out"),
            TimeoutError(),
            _http_error(429),
            _ok_response(),
        ]

        assert ddf.query_datadog("q", 0, 1) == {"series": []}
        assert mock_urlopen.call_count == 4

    def test_does_not_retry_client_error(
        self, mock_urlopen: Mock, mock_sleep: Mock
    ) -> None:
        mock_urlopen.side_effect = [_http_error(403)]

        self.assertRaises(HTTPError, ddf.query_datadog, "q", 0, 1)
        assert mock_urlopen.call_count == 1
        mock_sleep.assert_not_called()

    def test_gives_up_after_max_attempts(
        self, mock_urlopen: Mock, mock_sleep: Mock
    ) -> None:
        mock_urlopen.side_effect = _http_error(503)

        self.assertRaises(HTTPError, ddf.query_datadog, "q", 0, 1)
        assert mock_urlopen.call_count == ddf.QUERY_MAX_ATTEMPTS
        assert mock_sleep.call_count == ddf.QUERY_MAX_ATTEMPTS - 1
