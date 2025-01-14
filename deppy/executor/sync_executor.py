from typing import Sequence, Set

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor


class SyncExecutor(Executor):
    # TODO: add threading
    def __init__(self, deppy) -> None:
        super().__init__(deppy)

    def execute_node_with_scope_sync(self, node: Node, scope: Scope) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = [node.call_sync(**args) for args in call_args]
        return self.save_results(node, list(results), scope)

    def execute_node_sync(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = [self.execute_node_with_scope_sync(node, scope) for scope in scopes]
        self.scope_map[node] = set.union(*new_scopes)
        self.mark_complete(node)

    def execute_sync(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)
        ready_nodes = self.get_ready_nodes()

        tasks = ready_nodes
        while tasks:
            current_tasks = tasks
            tasks = set()

            for node in current_tasks:
                self.execute_node_sync(node)

            for node in current_tasks:
                successors = self.qualified_successors(node)
                tasks.update(successors)

        return self.root
