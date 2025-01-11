import asyncio
from typing import Dict, Any, List, Sequence
from networkx import MultiDiGraph

from .node import Node
from .scope import Scope


class Executor:
    def __init__(self, graph: MultiDiGraph) -> None:
        self.graph = graph
        self.flow_graph = None

    async def node_task(self, node: Node, scope: Scope, args: Dict[str, Any], work_graph: MultiDiGraph) -> None:
        result = await node(**args)
        scopes = set()
        if not node.loop_vars:
            scope[node] = result
            scopes.add(scope)
        else:
            child = scope.birth()
            child[node] = result
            scopes.add(child)

        successors = self.flow_graph.successors(node)
        if node in work_graph:
            work_graph.remove_node(node)
        qualified_nodes = [successor for successor in successors if work_graph.in_degree(successor) == 0]
        await asyncio.gather(*[self.execute_node(successor, scope, work_graph) for successor in qualified_nodes for scope in scopes])

    async def solo_race(self, node: Node, scope: Scope, work_graph: MultiDiGraph) -> None:
        args_list = await self._resolve_args(node, scope)

        if node.loop_vars:
            scope = scope.birth()
            scope["scope_name"] = str(node)

        await asyncio.gather(*[self.node_task(node, scope, single_args, work_graph.copy()) for single_args in args_list])

    async def team_race(self, node: Node, scope: Scope, work_graph: MultiDiGraph) -> None:
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
        if node in work_graph:
            work_graph.remove_node(node)
        qualified_nodes = [successor for successor in successors if work_graph.in_degree(successor) == 0]
        await asyncio.gather(*[self.execute_node(successor, scope, work_graph) for successor in qualified_nodes for scope in scopes])

    async def execute_node(self, node: Node, scope: Scope, work_graph: MultiDiGraph) -> None:
        if node.team_race:
            await self.team_race(node, scope, work_graph)
        else:
            await self.solo_race(node, scope, work_graph)

    def create_work_graph(self, *target_nodes) -> MultiDiGraph:
        work_graph = self.graph.copy()
        if len(target_nodes) == 0:
            return work_graph

        relevant_nodes = set()
        new_nodes = set(target_nodes)
        while new_nodes:
            relevant_nodes.update(new_nodes)
            newer = set()
            for node in new_nodes:
                newer.update(work_graph.predecessors(node))
            new_nodes = newer
        irrelevant_nodes = set(work_graph) - relevant_nodes
        work_graph.remove_nodes_from(irrelevant_nodes)
        return work_graph

    async def execute(self, *target_nodes: Sequence[Node]) -> Scope:
        """Execute the graph layer by layer."""
        work_graph = self.create_work_graph(*target_nodes)
        self.flow_graph = work_graph.copy()
        root = Scope()

        qualified_nodes = [node for node in work_graph if work_graph.in_degree(node) == 0]
        await asyncio.gather(*[self.execute_node(node, root, work_graph) for node in qualified_nodes])
        return root

    async def _resolve_args(self, node: Node, scope: Scope) -> List[Dict[str, Any]]:
        resolved_args = {}

        for pred in self.graph.predecessors(node):
            pred_data = scope[pred]
            for key, edge_data in self.graph.get_edge_data(pred, node).items():
                if (extractor := edge_data.get("extractor")) is not None:
                    resolved_args[key] = extractor(pred_data)
                else:
                    resolved_args[key] = pred_data

        if node.loop_vars:
            loop_keys = [key for key, _ in node.loop_vars]
            loop_values = (resolved_args[key] for key in loop_keys)
            return [
                {**resolved_args, **dict(zip(loop_keys, combination))}
                for combination in node.loop_strategy(*loop_values)
            ]

        return [resolved_args]
