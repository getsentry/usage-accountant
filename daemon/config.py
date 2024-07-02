import yaml
import pathlib
from typing import Iterator


class Config:
    def __init__(self, raw: str) -> None:
        self._parsed = yaml.safe_load_all(raw)

    @staticmethod
    def from_file(path: str) -> "Config":
        file = pathlib.Path(path)
        if not file.is_file():
            raise Exception("Configuraiton file doesnt exists")
        with file.open() as f:
            return Config(f.read())

    @staticmethod
    def from_string(string: str) -> "Config":
        return Config(string)

    def __iter__(self) -> Iterator:
        return self._parsed.__iter__()
