import functools
import json
import asyncio
from pathlib import Path
from typing import Callable, Any, Optional, Iterable, Union, Dict


class NotSetType:
    pass


not_set = NotSetType()


class StatedKwargs:
    def __init__(self, state_file: str = "state.json"):
        self.state_file_path = Path(state_file)
        self.state = {}

    def _load_state(self):
        if self.state_file_path.exists():
            with self.state_file_path.open("r") as f:
                return json.load(f)
        return {}

    def _save(self):
        with self.state_file_path.open("w") as f:
            json.dump(self.state, f, default=str)

    def _get(self, func: Callable, key: str, default=None):
        f_name = func.__name__
        f_dict = self.state.get(f_name)
        if f_dict is None:
            return default
        return f_dict.get(key, default)

    def _set(self, func: Callable, key: str, value: Any):
        f_name = func.__name__
        if f_name not in self.state:
            self.state[f_name] = {}
        self.state[f_name][key] = value

    def __enter__(self):
        self.state = self._load_state()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._save()

    def _manage_state(
        self,
        name: str,
        produce_function: Callable,
        initial_value: Any,
        keys: Union[Iterable[str], None],
        kwargs: Dict[str, Any],
        func: Callable,
    ):
        state_key = name
        if keys:
            state_key += ":" + ":".join(str(kwargs[k]) for k in keys if k in kwargs)

        if self._get(func, state_key, not_set) is not_set:
            if initial_value is not_set:
                self._set(func, state_key, produce_function())
            else:
                self._set(func, state_key, initial_value)

        return self._get(func, state_key), state_key

    def _update_state(
        self,
        state_key: str,
        produce_function: Callable,
        result: Any,
        from_result: bool,
        from_prev_state: bool,
        func: Callable,
    ):
        if from_result:
            self._set(func, state_key, produce_function(result))
        elif from_prev_state:
            self._set(func, state_key, produce_function(self._get(func, state_key)))
        else:
            self._set(func, state_key, produce_function())

    def stated_kwarg(
        self,
        name: str,
        produce_function: Callable,
        initial_value: Optional[Any] = not_set,
        from_result: Optional[bool] = False,
        from_prev_state: Optional[bool] = False,
        keys: Optional[Iterable[str]] = None,
    ):
        def decorator(func):
            return self(
                func,
                name,
                produce_function,
                initial_value,
                from_result,
                from_prev_state,
                keys,
            )

        return decorator

    def __call__(
        self,
        func,
        name: str,
        produce_function: Callable,
        initial_value: Optional[Any] = not_set,
        from_result: Optional[bool] = False,
        from_prev_state: Optional[bool] = False,
        keys: Optional[Iterable[str]] = None,
    ):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            val, state_key = self._manage_state(
                name, produce_function, initial_value, keys, kwargs, func
            )
            kwargs[name] = val
            result = func(*args, **kwargs)
            self._update_state(
                state_key, produce_function, result, from_result, from_prev_state, func
            )
            return result

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            val, state_key = self._manage_state(
                name, produce_function, initial_value, keys, kwargs, func
            )
            kwargs[name] = val
            result = await func(*args, **kwargs)
            self._update_state(
                state_key, produce_function, result, from_result, from_prev_state, func
            )
            return result

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
