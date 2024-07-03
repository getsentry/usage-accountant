from typing import List, Dict, Any
import importlib
from .base import Fetcher


__modules: List = {}


def FetcherFactory(_type: str, **kwargs: Dict[str, Any]) -> Fetcher:
    module_name = _type.lower()
    class_name = module_name.title()

    if module_name not in __modules:
        __modules[module_name] = importlib.import_module(f".{module_name}", __name__)

    module = __modules[module_name]
    klass = getattr(module, class_name)
    return klass(**kwargs)


__all__ = (FetcherFactory,)
