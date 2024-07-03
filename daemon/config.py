from __future__ import annotations
import yaml
import pathlib
from typing import NamedTuple, List, Mapping, Any, Sequence


class KafkaConfig(NamedTuple):
    bootstrap_servers: Sequence[str]
    config_params: Mapping[str, Any]


class FetcherConfig(NamedTuple):
    type: str
    period: float
    args: Mapping[str, Any]


class Config:
    """
    Expected config structure

    kafka: KafkaConfig
    fetchers: Sequence[FetcherConfig]
    """

    def __init__(self, raw: str) -> None:
        _parsed = yaml.safe_load(raw)
        try:
            self._kafka_topic = _parsed["kafka"]["topic"]
        except KeyError:
            raise KeyError("Invalid configuration: no topic provided")
        try:
            self._kafka_config = KafkaConfig(
                _parsed["kafka"]["bootstrap_servers"],
                _parsed["kafka"].get("config_params", {}),
            )
        except KeyError:
            raise KeyError("Invalid configuration: no bootstrap servers")

        self._fetchers = [
            FetcherConfig(f["type"], f["period"], f["args"])
            for f in _parsed["fetchers"]
        ]

    @staticmethod
    def from_file(path: str) -> Config:
        file = pathlib.Path(path)
        if not file.is_file():
            raise Exception("Configuraiton file doesnt exists")
        with file.open() as f:
            return Config(f.read())

    @staticmethod
    def from_string(string: str) -> Config:
        return Config(string)

    def get_kafka_topic(self) -> str:
        return self._kafka_topic

    def get_kafka_config(self) -> KafkaConfig:
        return self._kafka_config

    def get_fetchers(self) -> List[FetcherConfig]:
        return self._fetchers
