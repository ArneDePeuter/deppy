from typing import Sequence, Set, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Optional


from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class SyncExecutor(Executor):
    def __init__(
        self, deppy, max_thread_workers: Optional[int] = None, *args, **kwargs
    ) -> None:
        super().__init__(deppy)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_thread_workers)

    def shutdown(self):
        self.thread_pool.shutdown()

    def execute_node_with_scope_sync(self, node: Node, scope: Scope) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = [node.call_sync(**args) for args in call_args]
        return self.save_results(node, list(results), scope)

    def execute_node_sync(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = [
            self.execute_node_with_scope_sync(node, scope) for scope in scopes
        ]
        self.scope_map[node] = set.union(*new_scopes)

    def gather_thread_tasks(self, node: Node) -> Dict[Future, Tuple[Node, Scope]]:
        task_map = {}
        for scope in self.get_call_scopes(node):
            call_args = self.resolve_args(node, scope)
            for args in call_args:
                task = self.thread_pool.submit(node.call_sync, **args)
                task_map[task] = (node, scope)
        return task_map

    def execute_threaded_nodes(self, nodes: Set[Node]):
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
        threaded_nodes = {node for node in nodes if node.to_thread}
        sync_nodes = nodes - threaded_nodes

        self.execute_threaded_nodes(threaded_nodes)
        for node in sync_nodes:
            self.execute_node_sync(node)

    def execute_sync(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)

        for tasks in self.batched_topological_order():
            self.execute_nodes_sync(tasks)

        return self.root
