import asyncio
from typing import Dict, Any, List, Sequence
from networkx import MultiDiGraph

from .node import Node
from .scope import Scope


class Executor:
    def __init__(self, graph: MultiDiGraph) -> None:
        self.graph = graph
        self.root = Scope()
        self.work_graph = self.graph.copy()
        self.flow_graph = self.graph.copy()

    async def execute_node(self, node: Node, scope: Scope) -> None:
        args_list = await self._resolve_args(node, scope)
        results = await asyncio.gather(*[node(**single_args) for single_args in args_list])

        scopes = set()
        if not node.loop_vars:
            scope[node] = results[0]
            scopes.add(scope)
        else:
            scope = scope.birth()
            scope["scope_name"] = str(node)
            for result in results:
                child = scope.birth()
                child[node] = result
                scopes.add(child)

        successors = self.flow_graph.successors(node)
        if node in self.work_graph:
            self.work_graph.remove_node(node)
        qualified_nodes = [successor for successor in successors if self.work_graph.in_degree(successor) == 0]
        await asyncio.gather(*[self.execute_node(successor, scope) for successor in qualified_nodes for scope in scopes])

    def subgraph_work_graph(self, *target_nodes):
        relevant_nodes = set()
        new_nodes = set(target_nodes)
        while new_nodes:
            relevant_nodes.update(new_nodes)
            newer = set()
            for node in new_nodes:
                newer.update(self.work_graph.predecessors(node))
            new_nodes = newer
        irrelevant_nodes = set(self.work_graph) - relevant_nodes
        self.work_graph.remove_nodes_from(irrelevant_nodes)

    async def execute(self, *target_nodes: Sequence[Node]) -> Scope:
        """Execute the graph layer by layer."""
        self.work_graph = self.graph.copy()
        if len(target_nodes) > 0:
            self.subgraph_work_graph(*target_nodes)
        self.flow_graph = self.work_graph.copy()

        qualified_nodes = [node for node in self.work_graph if self.work_graph.in_degree(node) == 0]
        await asyncio.gather(*[self.execute_node(node, self.root) for node in qualified_nodes])
        return self.root

    @staticmethod
    async def _resolve_args(node: Node, scope: Scope) -> List[Dict[str, Any]]:
        resolved_args = {}

        for name, dep in node.dependencies.items():
            if isinstance(dep, Node):
                resolved_args[name] = scope[dep]
            elif callable(dep):
                resolved_args[name] = await asyncio.to_thread(dep)
            else:
                resolved_args[name] = dep

        if node.loop_vars:
            loop_keys = [name for name, _ in node.loop_vars]
            loop_values = [scope[dep] if isinstance(dep, Node) else dep for _, dep in node.loop_vars]
            return [{**resolved_args, **dict(zip(loop_keys, combination))} for combination in node.loop_strategy(*loop_values)]

        return [resolved_args]
