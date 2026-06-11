import json
import os
from logging import Logger

from typing import Mapping, NamedTuple, Optional, Sequence, TextIO

from usageaccountant.accumulator import (
    UsageAccumulator,
    KafkaConfig,
    UsageUnit,
)


class UsageAccumulatorRecord(NamedTuple):
    resource_id: str
    app_feature: str
    amount: int
    usage_type: UsageUnit
    timestamp: Optional[int]


def parse_and_assert_kafka_config(kafka_config_file: TextIO) -> KafkaConfig:
    """
    Validates if the kafka config file contains
    bootstrap_servers and config_params.
    """
    kafka_config = json.loads(kafka_config_file.read())

    assert isinstance(kafka_config, Mapping)
    assert "bootstrap_servers" in kafka_config
    assert "config_params" in kafka_config

    bootstrap_servers = kafka_config["bootstrap_servers"]
    config_params = kafka_config["config_params"]

    if "sasl.password" in config_params:
        config_params["sasl.password"] = os.path.expandvars(
            config_params["sasl.password"]
        )

    assert isinstance(bootstrap_servers, Sequence)
    assert isinstance(config_params, Mapping)

    return KafkaConfig(
        bootstrap_servers=bootstrap_servers, config_params=config_params
    )


def assert_valid_unit(unit: str) -> None:
    """
    Validates if unit provided is supported by UsageUnit enum.
    """
    UsageUnit(unit.lower())


def post_to_usage_accumulator(
    record_list: Sequence[UsageAccumulatorRecord],
    usage_accumulator: UsageAccumulator,
) -> None:
    """
    Posts UsageAccumulatorRecords to the UsageAccumulator.
    """
    for record in record_list:
        usage_accumulator.record(
            resource_id=record.resource_id,
            app_feature=record.app_feature,
            amount=record.amount,
            usage_type=record.usage_type,
            timestamp=record.timestamp,
        )


def log_records(
    logger: Logger, record_list: Sequence[UsageAccumulatorRecord]
) -> None:
    """
    Logs UsageAccumulatorRecords instead of producing them.
    """
    for record in record_list:
        logger.info(
            f"resource_id: {record.resource_id}, "
            f"app_feature: {record.app_feature}, "
            f"amount: {record.amount}, "
            f"usage_type: {record.usage_type}, "
            f"timestamp: {record.timestamp}."
        )
