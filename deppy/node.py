import asyncio
from itertools import product
from typing import Any, Tuple, Callable, Iterable, Sequence, Union, Type, Optional

LoopStrategy = Union[Callable[[Sequence[Any]], Iterable[Tuple[Any]]], Type[zip]]


class NodeFunctionError(Exception):
    def __init__(self, node, *args, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs)

    def __str__(self):
        return f"Error executing node function of {self.node}"


class Node:
    def __init__(
        self,
        func: Callable[..., Any],
        loop_strategy: Optional[LoopStrategy] = product,
        to_thread: Optional[bool] = False,
        name: Optional[str] = None,
        secret: Optional[bool] = False,
    ):
        self.func = func
        self.loop_vars = []
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.name = name or func.__name__
        self.secret = secret

    @property
    def is_async(self) -> bool:
        return asyncio.iscoroutinefunction(self.func)

    def call_sync(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            raise NodeFunctionError(self, e) from e

    async def call_async(self, *args, **kwargs):
        try:
            return await self.func(*args, **kwargs)
        except Exception as e:
            raise NodeFunctionError(self, e) from e

    def __repr__(self):
        return f"<Node {self.name}>"

    def __str__(self):
        return self.name
