from abc import ABC, abstractmethod
from typing import Iterable, Callable, Any, Awaitable

from functools import partial


class CallStrategy(ABC):
    @abstractmethod
    async def __call__(self, func: Callable[..., Any], **kwargs):
        ...


class SequentialCallStrategy(CallStrategy):
    def __init__(self, strategies: Iterable[CallStrategy]):
        self.strategies = strategies

    async def __call__(self, func: Callable[..., Awaitable[Any]], **kwargs):
        # wrap the callable with each strategy
        for strategy in self.strategies:
            func = partial(strategy, func=func)
        return await func(**kwargs)
