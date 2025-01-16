import asyncio
from typing import Sequence, Set, Optional, Any

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class AsyncExecutor(Executor):
    def __init__(
        self, deppy, max_concurrent_tasks: Optional[int] = None, *args, **kwargs
    ) -> None:
        super().__init__(deppy)
        if max_concurrent_tasks:
            self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
            self.call_node_async = self._call_with_semaphore
        else:
            self.semaphore = None
            self.call_node_async = self._call_without_semaphore

    async def _call_with_semaphore(self, node, *args, **kwargs) -> Any:
        async with self.semaphore:
            return await node.call_async(*args, **kwargs)

    @staticmethod
    async def _call_without_semaphore(node, *args, **kwargs) -> Any:
        return await node.func(*args, **kwargs)

    async def execute_node_with_scope_async(
        self, node: Node, scope: Scope
    ) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = await asyncio.gather(
            *[self.call_node_async(node, **args) for args in call_args]
        )
        return self.save_results(node, list(results), scope)

    async def execute_node_async(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = await asyncio.gather(
            *[self.execute_node_with_scope_async(node, scope) for scope in scopes]
        )
        self.scope_map[node] = set.union(*new_scopes)

    async def execute_async(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)

        for tasks in self.batched_topological_order():
            await asyncio.gather(*[self.execute_node_async(node) for node in tasks])

        return self.root
