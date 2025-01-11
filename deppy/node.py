import asyncio
from itertools import product
from typing import Any, Tuple, Callable, Iterable, Sequence, Union, Type, Optional


LoopStrategy = Union[Callable[[Sequence[Any]], Iterable[Tuple[Any]]], Type[zip]]


class Node:
    def __init__(
            self,
            func: Callable[..., Any],
            deppy: 'Deppy',
            loop_strategy: Optional[LoopStrategy] = product,
            to_thread: Optional[bool] = False,
            team_race: Optional[bool] = True,
            name: Optional[str] = None
    ):
        self.func = func
        self.deppy = deppy
        self.loop_vars = []
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
        return await self.execute(**kwargs)

    def __repr__(self):
        return f"<Node {self.name}>"

    def __str__(self):
        return self.name

    def __getattr__(self, name: str) -> Callable[[Any], 'Node']:
        def setter(dependency, loop: Optional[bool] = False, extractor: Optional[Callable[[Any], Any]] = None):
            self.deppy.edge(dependency, self, name, loop=loop, extractor=extractor)
            return self
        return setter
