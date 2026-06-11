import os
import unittest
from io import StringIO


from usageaccountant import accumulator
from usageaccountant import fetcher_utils as fu


class TestFetcherUtils(unittest.TestCase):
    kafka_config_str = (
        '{"bootstrap_servers": ["kafka.service.host:1234"],'
        '"config_params": '
        '{"compression.type": "lz4",'
        '"message.max.bytes": 10}}'
    )

    def test_parse_and_assert_kafka_config(self) -> None:
        kafka_io = StringIO(self.kafka_config_str)
        fu.parse_and_assert_kafka_config(kafka_io)

    def test_parse_and_assert_kafka_config_no_config_params(self) -> None:
        kafka_io = StringIO(
            '{"bootstrap_servers": ["kafka.service.host:1234"]}'
        )
        self.assertRaises(
            AssertionError, fu.parse_and_assert_kafka_config, kafka_io
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
            fu.parse_and_assert_kafka_config(kafka_io), expected_kafka_config
        )

    def test_assert_valid_unit(self) -> None:
        for unit in accumulator.UsageUnit:
            fu.assert_valid_unit(unit.value.upper())

    def test_assert_valid_unit_invalid(self) -> None:
        with self.assertRaises(ValueError):
            fu.assert_valid_unit("parsecs")
