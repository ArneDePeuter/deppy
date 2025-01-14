import asyncio
from typing import Sequence, Set

from .node import Node
from .scope import Scope
from .executor import Executor
from .deppy import Deppy


class AsyncExecutor(Executor):
    def __init__(self, deppy: Deppy) -> None:
        super().__init__(deppy)

    @staticmethod
    async def call_node(node: Node, **kwargs):
        if node.is_async:
            return await node.call_async(**kwargs)
        if node.to_thread:
            return await asyncio.to_thread(node.call_sync, **kwargs)
        return node.call_sync(**kwargs)

    async def execute_node_with_scope(self, node: Node, scope: Scope) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = await asyncio.gather(*[self.call_node(node, **args) for args in call_args])
        return self.save_results(node, list(results), scope)

    async def execute_node(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = await asyncio.gather(*[self.execute_node_with_scope(node, scope) for scope in scopes])
        self.scope_map[node] = set.union(*new_scopes)
        self.mark_complete(node)
        await asyncio.gather(*[self.execute_node(successor) for successor in self.qualified_successors(node)])

    async def execute(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)
        ready_nodes = self.get_ready_nodes()

        await asyncio.gather(*[self.execute_node(node) for node in ready_nodes])

        return self.root
