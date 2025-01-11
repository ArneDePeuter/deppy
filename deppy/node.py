import json
import asyncio
from itertools import product
from typing import Any, Tuple, Callable, Iterable, Sequence, Union, Type, Awaitable, Optional, Dict

from .cache import Cache


LoopStrategy = Union[Callable[[Sequence[Any]], Iterable[Tuple[Any]]], Type[zip]]
AnyFunc = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]


class Node:
    def __init__(
            self,
            func: AnyFunc,
            deppy: 'Deppy',
            loop_strategy: Optional[LoopStrategy] = product,
            cache: Optional[Cache] = None,
            to_thread: Optional[bool] = False,
            team_race: Optional[bool] = True,
            name: Optional[str] = None
    ):
        self.func = func
        self.deppy = deppy
        self.loop_vars = []
        self.cache = cache
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.team_race = team_race
        self.name = name or func.__name__

    async def __call__(self, **kwargs):
        if self.cache:
            cache_key = self.create_cache_key(kwargs)
            value, hit = self.cache.get(cache_key)
            if hit:
                return value

        if asyncio.iscoroutinefunction(self.func):
            res = await self.func(**kwargs)
        elif self.to_thread:
            res = await asyncio.to_thread(self.func, **kwargs)
        else:
            res = self.func(**kwargs)

        if self.cache:
            self.cache.set(cache_key, res)

        return res

    def __repr__(self):
        return f"<Node {self.name}>"

    def __str__(self):
        return self.name

    def __getattr__(self, name: str) -> Callable[[Any], 'Node']:
        def setter(dependency, loop: Optional[bool] = False, extractor: Optional[Callable[[Any], Any]] = None):
            self.deppy.edge(dependency, self, name, loop=loop, extractor=extractor)
            return self
        return setter

    @staticmethod
    def create_cache_key(args: Dict[str, Any]) -> str:
        try:
            return json.dumps(args, sort_keys=True, default=str)
        except TypeError:
            return str(sorted(args.items()))
