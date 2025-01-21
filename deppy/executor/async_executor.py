import asyncio
from typing import Sequence, Set, Optional, Any

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class AsyncExecutor(Executor):
    """
    An asynchronous executor for executing nodes in dependency graphs.

    Attributes
    ----------
    semaphore : Optional[asyncio.Semaphore]
        A semaphore to limit the number of concurrent tasks, or None if no limit is set.
    call_node_async : Callable
        The method used to call nodes asynchronously, adjusted based on semaphore usage.
    """

    def __init__(
        self, deppy, max_concurrent_tasks: Optional[int] = None, *args, **kwargs
    ) -> None:
        """
        Constructs an AsyncExecutor instance.

        Parameters
        ----------
        deppy : Any
            The main dependency manager instance.
        max_concurrent_tasks : Optional[int], optional
            Maximum number of concurrent tasks (default is None, meaning unlimited).
        """
        super().__init__(deppy)
        if max_concurrent_tasks:
            self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
            self.call_node_async = self._call_with_semaphore
        else:
            self.semaphore = None
            self.call_node_async = self._call_without_semaphore

    async def _call_with_semaphore(self, node, *args, **kwargs) -> Any:
        """
        Calls a node asynchronously, respecting the semaphore limit.

        Parameters
        ----------
        node : Node
            The node to call.
        *args : Any
            Positional arguments for the node call.
        **kwargs : Any
            Keyword arguments for the node call.

        Returns
        -------
        Any
            The result of the node call.
        """
        async with self.semaphore:
            return await node.call_async(*args, **kwargs)

    @staticmethod
    async def _call_without_semaphore(node, *args, **kwargs) -> Any:
        """
        Calls a node asynchronously without any concurrency limit.

        Parameters
        ----------
        node : Node
            The node to call.
        *args : Any
            Positional arguments for the node call.
        **kwargs : Any
            Keyword arguments for the node call.

        Returns
        -------
        Any
            The result of the node call.
        """
        return await node.func(*args, **kwargs)

    async def execute_node_with_scope_async(
        self, node: Node, scope: Scope
    ) -> Set[Scope]:
        """
        Executes a single node asynchronously within a given scope.

        Parameters
        ----------
        node : Node
            The node to execute.
        scope : Scope
            The scope in which the node is executed.

        Returns
        -------
        Set[Scope]
            A set of scopes resulting from the node's execution.
        """
        call_args = self.resolve_args(node, scope)
        results = await asyncio.gather(
            *[self.call_node_async(node, **args) for args in call_args]
        )
        return self.save_results(node, list(results), scope)

    async def execute_node_async(self, node: Node) -> None:
        """
        Executes a single node asynchronously across all its applicable scopes.

        Parameters
        ----------
        node : Node
            The node to execute.
        """
        scopes = self.get_call_scopes(node)
        if len(scopes) == 0:
            return
        new_scopes = await asyncio.gather(
            *[self.execute_node_with_scope_async(node, scope) for scope in scopes]
        )
        self.scope_map[node] = set.union(*new_scopes)

    async def execute_async(self, *target_nodes: Sequence[Node]) -> Scope:
        """
        Executes the dependency graph asynchronously.

        Parameters
        ----------
        target_nodes : Sequence[Node]
            The target nodes to execute.

        Returns
        -------
        Scope
            The root scope containing the execution results.
        """
        self.setup(*target_nodes)

        for tasks in self.batched_topological_order():
            await asyncio.gather(*[self.execute_node_async(node) for node in tasks])

        return self.root
