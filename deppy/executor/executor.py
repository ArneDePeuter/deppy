from typing import Dict, Any, List, Sequence, Set, Iterator
from networkx import MultiDiGraph

from deppy.node import Node
from deppy.scope import Scope
from deppy.ignore_result import IgnoreResult


class Executor:
    """
    Baseclass to execute dependency graphs with scope and flow management.

    Attributes
    ----------
    deppy : Any
        The deppy instance.
    scope_map : Dict[Node, Set[Scope]]
        Maps nodes to their corresponding scopes.
    root : Scope
        The root scope for the execution.
    flow_graph : MultiDiGraph
        The directed acyclic graph representing the dependency flow of execution.
    """

    def __init__(self, deppy) -> None:
        """
        Constructs an Executor instance.

        Parameters
        ----------
        deppy : Any
            The deppy instance.
        """
        self.deppy = deppy
        self.scope_map: Dict[Node, Set[Scope]] = {}
        self.root: Scope = Scope()
        self.flow_graph: MultiDiGraph = MultiDiGraph()

    def batched_topological_order(self) -> Iterator[Set[Node]]:
        """
        Yields sets of nodes in topological order for parallel processing.

        Returns
        -------
        Iterator[Set[Node]]
            An iterator yielding sets of nodes that can be processed in parallel.
        """
        in_degree = {node: self.flow_graph.in_degree(node) for node in self.flow_graph}

        while in_degree:
            ready_nodes = {node for node, degree in in_degree.items() if degree == 0}

            yield ready_nodes

            for node in ready_nodes:
                del in_degree[node]
                for successor in self.flow_graph.successors(node):
                    in_degree[successor] -= 1

    @staticmethod
    def save_results(node: Node, results: List[Any], scope: Scope) -> Set[Scope]:
        """
        Saves the results of a node execution into the corresponding scopes.
        Also generating new scopes for looped nodes.

        Parameters
        ----------
        node : Node
            The node whose results are being saved.
        results : List[Any]
            The results to save.
        scope : Scope
            The scope where results should be saved.

        Returns
        -------
        Set[Scope]
            The set of scopes containing the saved results.
        """
        scopes = set()
        if not node.loop_vars:
            scope[node] = results[0]
            if not isinstance(results[0], IgnoreResult):
                scopes.add(scope)
            return scopes
        else:
            sub = scope.birth()
            for result in results:
                child = sub.birth()
                child[node] = result
                if not isinstance(result, IgnoreResult):
                    scopes.add(child)
        return scopes

    def create_flow_graph(self, *target_nodes: Sequence[Node]) -> MultiDiGraph:
        """
        Creates a subgraph containing only the relevant nodes for the target nodes.

        Parameters
        ----------
        target_nodes : Sequence[Node]
            The nodes for which the subgraph should be created.

        Returns
        -------
        MultiDiGraph
            The subgraph containing only relevant nodes.
        """
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
        """
        Sets up the flow graph and root scope for execution.

        Parameters
        ----------
        target_nodes : Sequence[Node]
            The target nodes for the execution setup.
        """
        self.flow_graph = self.create_flow_graph(*target_nodes)
        self.root = Scope()
        self.scope_map = {}

    def get_call_scopes(self, node: Node) -> Set[Scope]:
        """
        Determines the scopes to use for a node's execution.

        Parameters
        ----------
        node : Node
            The node for which to determine the scopes.

        Returns
        -------
        Set[Scope]
            The set of scopes to use for the node's execution.
        """
        preds = list(self.flow_graph.predecessors(node))
        if not preds:
            return {self.root}
        all_scopes = [self.scope_map[pred] for pred in preds]
        if any(len(scope) == 0 for scope in all_scopes):
            return set()
        scopes = all_scopes.pop()
        for iter_scopes in all_scopes:
            cur_scope = next(iter(scopes))
            qualifier_scope = next(iter(iter_scopes))
            if len(cur_scope.path) < len(qualifier_scope.path):  # pragma: no cover
                scopes = iter_scopes
        return scopes

    def resolve_args(self, node: Node, scope: Scope) -> List[Dict[str, Any]]:
        """
        Resolves arguments for a node's execution based on the scope.

        Parameters
        ----------
        node : Node
            The node whose arguments are being resolved.
        scope : Scope
            The scope to resolve arguments against.

        Returns
        -------
        List[Dict[str, Any]]
            A list of resolved argument dictionaries.
        """
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
