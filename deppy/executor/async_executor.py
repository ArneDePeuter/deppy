import asyncio
from typing import Sequence, Set

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class AsyncExecutor(Executor):
    def __init__(self, deppy) -> None:
        super().__init__(deppy)

    async def execute_node_with_scope_async(self, node: Node, scope: Scope) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = await asyncio.gather(*[node.call_async(**args) for args in call_args])
        return self.save_results(node, list(results), scope)

    async def execute_node_async(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = await asyncio.gather(*[self.execute_node_with_scope_async(node, scope) for scope in scopes])
        self.scope_map[node] = set.union(*new_scopes)
        self.mark_complete(node)

    async def execute_async(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)
        ready_nodes = self.get_ready_nodes()

        tasks = ready_nodes
        while tasks:
            current_tasks = tasks
            tasks = set()

            await asyncio.gather(*[self.execute_node_async(node) for node in current_tasks])

            for node in current_tasks:
                successors = self.qualified_successors(node)
                tasks.update(successors)

        return self.root