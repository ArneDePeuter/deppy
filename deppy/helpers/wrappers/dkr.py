from functools import wraps
from abc import ABC, abstractmethod
import re
from typing import Iterable, Set, Dict, Any, Union, Optional
from collections.abc import MutableMapping as Mapping
import asyncio


class Dk(ABC):
    """Dynamic keyword argument."""

    def __init__(self, keys: Set[str]):
        self.keys = keys

    @abstractmethod
    def resolve(self, data: Dict[str, Any]):  # pragma: no cover
        ...


class StringDk(Dk):
    def __init__(self, value: str):
        self.value = value
        keys = set(re.findall(r"\{(.*?)}", value))
        super().__init__(keys)

    def resolve(self, data):
        if self.value.startswith("{") and self.value.endswith("}"):
            return data.get(self.value[1:-1], self.value)
        return self.value.format(**data)


class MappingDk(Dk):
    def __init__(self, value: Mapping[Union[Any, Dk], Union[Any, Dk]]):
        self.value = value
        keys = self.gather_keys(value)
        super().__init__(keys)

    def gather_keys(self, value):
        keys = set()
        for k, v in value.items():
            if isinstance(k, Dk):
                keys.update(k.keys)
            if isinstance(v, Dk):
                keys.update(v.keys)
            if isinstance(v, Mapping):
                keys.update(self.gather_keys(v))
        return keys

    def resolve(self, data):
        result = type(self.value)()
        for k, v in self.value.items():
            if isinstance(k, Dk):
                k = k.resolve(data)
            if isinstance(v, Dk):
                v = v.resolve(data)
            result[k] = v
        return result


class IterDk(Dk):
    def __init__(self, value: Iterable[Union[Any, Dk]]):
        self.value = value
        keys = set()
        for v in value:
            if isinstance(v, Dk):
                keys.update(v.keys)
        super().__init__(keys)

    def resolve(self, data):
        return type(self.value)(
            v.resolve(data) if isinstance(v, Dk) else v for v in self.value
        )


class JsonDk(Dk):
    def __init__(self, value: Any):
        self.value, detected = self.emplace_if_detected(value)
        assert detected, "JsonDk must contain at least one dynamic keyword argument."
        keys = self.value.keys
        super().__init__(keys)

    def emplace_if_detected(self, value):
        if isinstance(value, str):
            dk = StringDk(value)
            if dk.keys:
                return dk, True
            else:
                return value, False
        elif isinstance(value, Mapping):
            has_dk = False
            for k, v in value.items():
                dk_v, detected_v = self.emplace_if_detected(v)
                dk_k, detected_k = self.emplace_if_detected(k)
                detected = detected_v or detected_k
                value[dk_k] = dk_v
                has_dk = has_dk or detected
            if has_dk:
                return MappingDk(value), True
            else:
                return value, False
        elif isinstance(value, Iterable):
            has_dk = False
            new_vals = []
            for v in value:
                dk, detected = self.emplace_if_detected(v)
                new_vals.append(dk)
                has_dk = has_dk or detected
            if has_dk:
                new_vals = type(value)(new_vals)
                return IterDk(new_vals), True
            else:
                return value, False
        return value, False

    def resolve(self, data):
        return self.value.resolve(data)


class Dkr:
    """Dynamic keyword argument resolver."""

    def __init__(self, **dk_dict):
        self.dk_dict = dk_dict

    def resolve(self, kwargs: Dict[str, Any]):
        """
        Resolve all dynamic keyword arguments using provided kwargs.
        """
        resolved_kwargs = {}
        for k, v in self.dk_dict.items():
            if isinstance(v, Dk):
                resolved_kwargs[k] = v.resolve(kwargs)
            else:
                resolved_kwargs[k] = v
        return resolved_kwargs

    def wraps(self, func, sub_name: Optional[str] = None):
        @wraps(func)
        async def async_wrapper(**kwargs):
            resolved_kwargs = self.resolve(kwargs)
            return await func(**resolved_kwargs)

        @wraps(func)
        def sync_wrapper(**kwargs):
            resolved_kwargs = self.resolve(kwargs)
            return func(**resolved_kwargs)

        wrapper = async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        if sub_name:
            wrapper.__name__ = f"{func.__name__}_{sub_name}"
        return wrapper

    def __call__(self, func, sub_name: Optional[str] = None):
        return self.wraps(func, sub_name)
