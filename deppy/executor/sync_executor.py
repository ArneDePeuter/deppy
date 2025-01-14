from typing import Sequence, Set

from deppy.node import Node
from deppy.scope import Scope
from .executor import Executor
from deppy.deppy import Deppy


class SyncExecutor(Executor):
    # TODO: add threading
    def __init__(self, deppy: Deppy) -> None:
        assert not deppy.has_async_nodes(), "Deppy has async nodes but SyncExecutor was constructed"
        super().__init__(deppy)

    def execute_node_with_scope(self, node: Node, scope: Scope) -> Set[Scope]:
        call_args = self.resolve_args(node, scope)
        results = [node.call_sync(**args) for args in call_args]
        return self.save_results(node, list(results), scope)

    def execute_node(self, node: Node) -> None:
        scopes = self.get_call_scopes(node)
        new_scopes = [self.execute_node_with_scope(node, scope) for scope in scopes]
        self.scope_map[node] = set.union(*new_scopes)
        self.mark_complete(node)

        for successor in self.qualified_successors(node):
            self.execute_node(successor)

    def execute(self, *target_nodes: Sequence[Node]) -> Scope:
        self.setup(*target_nodes)
        ready_nodes = self.get_ready_nodes()

        for node in ready_nodes:
            self.execute_node(node)

        return self.root
