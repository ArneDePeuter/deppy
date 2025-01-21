from functools import wraps
from abc import ABC, abstractmethod
import re
from typing import Iterable, Set, Dict, Any, Union, Optional
from collections.abc import MutableMapping as Mapping
import asyncio


class Dk(ABC):
    """
    Abstract Base Class for dynamic keyword arguments.

    Attributes
    ----------
    keys : Set[str]
        A set of dynamic keys used for resolution.
    """

    def __init__(self, keys: Set[str]):
        self.keys = keys

    @abstractmethod
    def resolve(self, data: Dict[str, Any]):  # pragma: no cover
        """
        Abstract method to resolve dynamic keys against provided data.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary containing values for dynamic key resolution.
        """
        ...


class StringDk(Dk):
    """
    Resolves dynamic keys within strings using Python's string formatting.
    If the string only contains a single dynamic key, it will be emplaced directly otherwise it will be formatted.

    Examples
    --------
    >>> dk = StringDk("Hello, {name}!")
    >>> dk.resolve({"name": "World"})
    "Hello, World!"

    >>> dk = StringDk("{greeting}, {name}!")
    >>> dk.resolve({"greeting": "Hello", "name": "World"})
    "Hello, World!"
    """

    def __init__(self, value: str):
        self.value = value
        keys = set(re.findall(r"\{(.*?)}", value))
        super().__init__(keys)

    def resolve(self, data):
        """
        Resolves the string using provided data.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary to resolve the dynamic keys.

        Returns
        -------
        str
            The resolved string.
        """
        if self.value.startswith("{") and self.value.endswith("}"):
            return data.get(self.value[1:-1], self.value)
        return self.value.format(**data)


class MappingDk(Dk):
    """
    Resolves dynamic keys within mappings (e.g., dictionaries).
    Resolves other Dks in its keys and values.

    Examples
    --------
    >>> dk = MappingDk({"name": StringDk("{first} {last}")})
    >>> dk.resolve({"first": "John", "last": "Doe"})
    {"name": "John Doe"}
    """

    def __init__(self, value: Mapping[Union[Any, Dk], Union[Any, Dk]]):
        self.value = value
        keys = self.gather_keys(value)
        super().__init__(keys)

    def gather_keys(self, value):
        """
        Recursively gathers dynamic keys from the mapping.
        """
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
        """
        Resolves the mapping using provided data.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary to resolve the dynamic keys.

        Returns
        -------
        Mapping
            The resolved mapping.
        """
        result = type(self.value)()
        for k, v in self.value.items():
            if isinstance(k, Dk):
                k = k.resolve(data)
            if isinstance(v, Dk):
                v = v.resolve(data)
            result[k] = v
        return result


class IterDk(Dk):
    """
    Resolves dynamic keys within iterables (e.g., lists, tuples).

    Examples
    --------
    >>> dk = IterDk([StringDk("{greeting}, {name}!"), "test"])
    >>> dk.resolve({"greeting": "Hello", "name": "World"})
    ["Hello, World!", "test"]
    """

    def __init__(self, value: Iterable[Union[Any, Dk]]):
        self.value = value
        keys = set()
        for v in value:
            if isinstance(v, Dk):
                keys.update(v.keys)
        super().__init__(keys)

    def resolve(self, data):
        """
        Resolves the iterable using provided data.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary to resolve the dynamic keys.

        Returns
        -------
        Iterable
            The resolved iterable.
        """
        return type(self.value)(
            v.resolve(data) if isinstance(v, Dk) else v for v in self.value
        )


class JsonDk(Dk):
    """
    Resolves dynamic keys within JSON-like structures.
    Automatically detecting dk within strings, mappings, and iterables.

    Examples
    --------
    >>> dk = JsonDk({"name": "{first} {last}", "iter": ["{greeting}, {name}!", "test"]})
    >>> dk.resolve({"first": "John", "last": "Doe", "greeting": "Hello"})
    {"name": "John Doe", "iter": ["Hello, John Doe!", "test"]}
    """

    def __init__(self, value: Any):
        self.value, detected = self.emplace_if_detected(value)
        assert detected, "JsonDk must contain at least one dynamic keyword argument."
        keys = self.value.keys
        super().__init__(keys)

    def emplace_if_detected(self, value):
        """
        Detects and wraps dynamic keys within the given value.
        """
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
        """
        Resolves the JSON-like structure using provided data.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary to resolve the dynamic keys.

        Returns
        -------
        Any
            The resolved JSON-like structure.
        """
        return self.value.resolve(data)


class Dkr:
    """
    A resolver for managing and resolving dynamic keyword arguments.
    """

    def __init__(self, **dk_dict):
        self.dk_dict = dk_dict

    def resolve(self, kwargs: Dict[str, Any]):
        """
        Resolves all dynamic keyword arguments using provided keyword arguments.

        Parameters
        ----------
        kwargs : Dict[str, Any]
            The keyword arguments for resolving dynamic keys.

        Returns
        -------
        Dict[str, Any]
            The resolved keyword arguments.
        """
        resolved_kwargs = {}
        for k, v in self.dk_dict.items():
            if isinstance(v, Dk):
                resolved_kwargs[k] = v.resolve(kwargs)
            else:
                resolved_kwargs[k] = v
        return resolved_kwargs

    def wraps(self, func, sub_name: Optional[str] = None):
        """
        Wraps a function to resolve dynamic keyword arguments before execution.

        Parameters
        ----------
        func : Callable
            The function to wrap.
        sub_name : Optional[str], optional
            A sub-name to append to the function's name (default is None).

        Returns
        -------
        Callable
            The wrapped function.
        """

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
        """
        Allows the resolver to be used as a decorator.
        """
        return self.wraps(func, sub_name)
