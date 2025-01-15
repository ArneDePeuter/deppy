from typing import Dict, Any, List, Sequence, Set
from networkx import MultiDiGraph

from deppy.node import Node
from deppy.scope import Scope
from deppy.ignore_result import IgnoreResult


class Executor:
    def __init__(self, deppy) -> None:
        self.deppy = deppy
        self.scope_map: Dict[Node, Set[Scope]] = {}
        self.in_degrees: Dict[Node, int] = {}
        self.root: Scope = Scope()
        self.flow_graph: MultiDiGraph = MultiDiGraph()

    def mark_complete(self, node: Node) -> None:
        del self.in_degrees[node]
        for successor in self.flow_graph.successors(node):
            self.in_degrees[successor] -= 1

    def get_ready_nodes(self) -> Set[Node]:
        return set(node for node, degree in self.in_degrees.items() if degree == 0)

    @staticmethod
    def save_results(node: Node, results: List[Any], scope: Scope) -> Set[Scope]:
        scopes = set()
        if not node.loop_vars:
            scope[node] = results[0]
            if not isinstance(results[0], IgnoreResult):
                scopes.add(scope)
            return scopes
        else:
            for result in results:
                child = scope.birth()
                child[node] = result
                if not isinstance(result, IgnoreResult):
                    scopes.add(child)
        return scopes

    def create_flow_graph(self, *target_nodes: Sequence[Node]) -> MultiDiGraph:
        flow_graph = self.deppy.graph.copy()
        if len(target_nodes) == 0:
            return flow_graph

        relevant_nodes = set()
        new_nodes = set(target_nodes)
        while new_nodes:
            relevant_nodes.update(new_nodes)
            newer = set()
            for node in new_nodes:
                newer.update(flow_graph.predecessors(node))
            new_nodes = newer
        irrelevant_nodes = set(flow_graph) - relevant_nodes
        flow_graph.remove_nodes_from(irrelevant_nodes)
        return flow_graph

    def setup(self, *target_nodes: Sequence[Node]) -> None:
        self.flow_graph = self.create_flow_graph(*target_nodes)
        self.in_degrees = {node: self.flow_graph.in_degree(node) for node in self.flow_graph}
        self.root = Scope()
        self.scope_map = {}

    def get_call_scopes(self, node: Node) -> Set[Scope]:
        all_scopes = [
            self.scope_map[pred]
            for pred in self.flow_graph.predecessors(node)
        ]
        if not all_scopes:
            return {self.root}
        scopes = all_scopes.pop()
        for iter_scopes in all_scopes:
            cur_scope = next(iter(scopes))
            qualifier_scope = next(iter(iter_scopes))
            assert cur_scope.common_branch(qualifier_scope), "Scope joining not implemented"
            if len(cur_scope.path) < len(qualifier_scope.path):
                scopes = iter_scopes
        return scopes

    def resolve_args(self, node: Node, scope: Scope) -> List[Dict[str, Any]]:
        resolved_args = {
            key: scope[pred]
            for pred, _, key in self.flow_graph.in_edges(node, keys=True)
        }

        if node.loop_vars:
            loop_keys = [key for key, _ in node.loop_vars]
            loop_values = (resolved_args[key] for key in loop_keys)
            return [
                {**resolved_args, **dict(zip(loop_keys, combination))}
                for combination in node.loop_strategy(*loop_values)
            ]

        return [resolved_args]