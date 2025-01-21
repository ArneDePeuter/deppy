from typing import Sequence, Set, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Optional

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class SyncExecutor(Executor):
    """
    A synchronous executor for executing nodes in dependency graphs.

    Attributes
    ----------
    thread_pool : ThreadPoolExecutor
        Thread pool used for parallel execution of nodes.
    """

    def __init__(
        self, deppy, max_thread_workers: Optional[int] = None, *args, **kwargs
    ) -> None:
        """
        Constructs a SyncExecutor instance.

        Parameters
        ----------
        deppy : Any
            The main dependency manager instance.
        max_thread_workers : Optional[int], optional
            Maximum number of threads in the thread pool (default is None, which allows unlimited threads).
        """
        super().__init__(deppy)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_thread_workers)

    def shutdown(self):
        """
        Shuts down the thread pool executor, releasing resources.
        """
        self.thread_pool.shutdown()

    def execute_node_with_scope_sync(self, node: Node, scope: Scope) -> Set[Scope]:
        """
        Executes a single node synchronously within a given scope.

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
        results = [node.call_sync(**args) for args in call_args]
        return self.save_results(node, list(results), scope)

    def execute_node_sync(self, node: Node) -> None:
        """
        Executes a single node synchronously across all its applicable scopes.

        Parameters
        ----------
        node : Node
            The node to execute.
        """
        scopes = self.get_call_scopes(node)
        if len(scopes) == 0:
            return
        new_scopes = [
            self.execute_node_with_scope_sync(node, scope) for scope in scopes
        ]
        self.scope_map[node] = set.union(*new_scopes)

    def gather_thread_tasks(self, node: Node) -> Dict[Future, Tuple[Node, Scope]]:
        """
        Gathers thread pool tasks for a threaded node's execution.

        Parameters
        ----------
        node : Node
            The node for which to gather tasks.

        Returns
        -------
        Dict[Future, Tuple[Node, Scope]]
            A mapping of Future objects to their corresponding node and scope.
        """
        task_map = {}
        for scope in self.get_call_scopes(node):
            call_args = self.resolve_args(node, scope)
            for args in call_args:
                task = self.thread_pool.submit(node.call_sync, **args)
                task_map[task] = (node, scope)
        return task_map

    def execute_threaded_nodes(self, nodes: Set[Node]):
        """
        Executes threaded nodes using the thread pool.

        Parameters
        ----------
        nodes : Set[Node]
            The set of nodes to execute using threads.
        """
        task_map = {}
        for node in nodes:
            task_map.update(self.gather_thread_tasks(node))

        for future in as_completed(task_map):
            result = future.result()
            node, scope = task_map[future]
            if node not in self.scope_map:
                self.scope_map[node] = set()
            new_scopes = self.save_results(node, [result], scope)
            self.scope_map[node].update(new_scopes)

    def execute_nodes_sync(self, nodes: Set[Node]) -> None:
        """
        Executes a set of nodes, either synchronously or using threads based on their properties.

        Parameters
        ----------
        nodes : Set[Node]
            The set of nodes to execute.
        """
        threaded_nodes = {node for node in nodes if node.to_thread}
        sync_nodes = nodes - threaded_nodes

        self.execute_threaded_nodes(threaded_nodes)
        for node in sync_nodes:
            self.execute_node_sync(node)

    def execute_sync(self, *target_nodes: Sequence[Node]) -> Scope:
        """
        Executes the dependency graph synchronously.

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
            self.execute_nodes_sync(tasks)

        return self.root
