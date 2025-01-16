from typing import Sequence, Optional
from deppy.node import Node
from deppy.scope import Scope
import asyncio

from .sync_executor import SyncExecutor
from .async_executor import AsyncExecutor


class HybridExecutor(AsyncExecutor, SyncExecutor):
    def __init__(
        self,
        deppy,
        max_thread_workers: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
    ) -> None:
        super().__init__(
            deppy,
            max_thread_workers=max_thread_workers,
            max_concurrent_tasks=max_concurrent_tasks,
        )

    async def execute_hybrid(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)

        for tasks in self.batched_topological_order():
            async_nodes = {node for node in tasks if node.is_async}
            sync_nodes = {node for node in tasks if not node.is_async}

            if async_nodes:
                await asyncio.gather(
                    *[self.execute_node_async(node) for node in async_nodes]
                )

            self.execute_nodes_sync(sync_nodes)

        return self.root
