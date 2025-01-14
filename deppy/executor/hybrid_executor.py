from typing import Sequence
from deppy.node import Node
from deppy.scope import Scope
import asyncio

from .sync_executor import SyncExecutor
from .async_executor import AsyncExecutor


class HybridExecutor(AsyncExecutor, SyncExecutor):
    def __init__(self, deppy) -> None:
        super().__init__(deppy)

    async def execute_hybrid(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)
        ready_nodes = self.get_ready_nodes()

        tasks = ready_nodes
        while tasks:
            current_tasks = tasks
            tasks = set()

            async_tasks = [node for node in current_tasks if node.is_async]
            sync_tasks = [node for node in current_tasks if not node.is_async]

            if async_tasks:
                await asyncio.gather(*[self.execute_node_async(node) for node in async_tasks])

            for node in sync_tasks:
                self.execute_node_sync(node)

            for node in current_tasks:
                successors = self.qualified_successors(node)
                tasks.update(successors)

        return self.root
