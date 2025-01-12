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

    def _get(self, key: str, default=None):
        return self.state.get(key, default)

    def _set(self, key: str, value: Any):
        self.state[key] = value

    def __enter__(self):
        self.state = self._load_state()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._save()

    def _manage_state(self, name: str, produce_function: Callable, initial_value: Any, keys: Union[Iterable[str], None], kwargs: Dict[str, Any]):
        state_key = name
        if keys:
            state_key += ":" + ":".join(str(kwargs[k]) for k in keys if k in kwargs)

        if state_key not in self.state:
            if initial_value is not_set:
                self._set(state_key, produce_function())
            else:
                self._set(state_key, initial_value)

        return self._get(state_key), state_key

    def _update_state(self, state_key: str, produce_function: Callable, result: Any, from_result: bool, from_prev_state: bool):
        if from_result:
            self._set(state_key, produce_function(result))
        elif from_prev_state:
            self._set(state_key, produce_function(self._get(state_key)))
        else:
            self._set(state_key, produce_function())

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
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                val, state_key = self._manage_state(name, produce_function, initial_value, keys, kwargs)
                kwargs[name] = val
                result = func(*args, **kwargs)
                self._update_state(state_key, produce_function, result, from_result, from_prev_state)
                return result

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                val, state_key = self._manage_state(name, produce_function, initial_value, keys, kwargs)
                kwargs[name] = val
                result = await func(*args, **kwargs)
                self._update_state(state_key, produce_function, result, from_result, from_prev_state)
                return result

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator
