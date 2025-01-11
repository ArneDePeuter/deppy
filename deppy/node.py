import asyncio
from itertools import product
from typing import Any, Tuple, Callable, Iterable, Sequence, Union, Type, Awaitable, Optional

from deppy.call_strategies import CallStrategy


LoopStrategy = Union[Callable[[Sequence[Any]], Iterable[Tuple[Any]]], Type[zip]]
AnyFunc = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]


class Node:
    def __init__(
            self,
            func: AnyFunc,
            deppy: 'Deppy',
            loop_strategy: Optional[LoopStrategy] = product,
            call_strategy: Optional[CallStrategy] = None,
            to_thread: Optional[bool] = False,
            team_race: Optional[bool] = True,
            name: Optional[str] = None
    ):
        self.func = func
        self.deppy = deppy
        self.loop_vars = []
        self.call_strategy = call_strategy
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.team_race = team_race
        self.name = name or func.__name__

    async def execute(self, **kwargs):
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(**kwargs)
        elif self.to_thread:
            return await asyncio.to_thread(self.func, **kwargs)
        return self.func(**kwargs)

    async def __call__(self, **kwargs):
        return await self.call_strategy(self.execute, **kwargs) if self.call_strategy is not None else await self.execute(**kwargs)

    def __repr__(self):
        return f"<Node {self.name}>"

    def __str__(self):
        return self.name

    def __getattr__(self, name: str) -> Callable[[Any], 'Node']:
        def setter(dependency, loop: Optional[bool] = False, extractor: Optional[Callable[[Any], Any]] = None):
            self.deppy.edge(dependency, self, name, loop=loop, extractor=extractor)
            return self
        return setter
