import asyncio
from typing import Dict, Any, List, Sequence, Iterable, Optional
from networkx import MultiDiGraph
from tqdm.asyncio import tqdm
from contextlib import contextmanager, asynccontextmanager


from .node import Node
from .scope import Scope
from .ignore_result import IgnoreResult


class Executor:
    def __init__(self, graph: MultiDiGraph) -> None:
        self.graph = graph
        self.flow_graph = None
        self.pbar = None
        self.mutex_map: Dict[Node, asyncio.Lock] = {}
        self.second_order_predecessor_map = {}
        self.scope_map: Dict[Node, Iterable[Scope]] = {}
        self.in_degrees = {}

    @contextmanager
    def update_pbar(self, new_nodes: int) -> None:
        self.pbar.total += new_nodes
        yield
        self.pbar.update(new_nodes)

    @asynccontextmanager
    async def acquire(self, node: Node) -> None:
        second_order_predecessors = self.second_order_predecessor_map.get(node, set())
        mutexes = [self.mutex_map[second_order_predecessor] for second_order_predecessor in second_order_predecessors]

        for mutex in mutexes:
            await mutex.acquire()
        yield
        for mutex in mutexes:
            mutex.release()

    async def execute_successors(self, node: Node, scopes: Iterable[Scope]) -> None:
        # this method is not concurrently safe because
        # if a a predecessor of one of the successors is removed while looping trough for the qualified nodes
        # it could be that this predecessor and this current node creates tasks, meaning too much tasks are created
        successor_scope_map = {}
        async with self.acquire(node):
            successors = list(self.flow_graph.successors(node))
            if node in self.in_degrees:
                del self.in_degrees[node]
                for successor in successors:
                    self.in_degrees[successor] -= 1

            # getting the correct scopes for the successors
            # optionally later merge scopes
            for successor in successors:
                relevant_scopes = self.scope_map.get(successor)
                if relevant_scopes is None:
                    self.scope_map[successor] = scopes
                    successor_scope_map[successor] = scopes
                else:
                    relevant_scope = next(iter(relevant_scopes))
                    new_scope = next(iter(scopes))
                    assert relevant_scope.is_family(new_scope), "Scope merging is not implemented"
                    # get the deepest scope
                    if len(new_scope.path) > len(relevant_scope.path):
                        self.scope_map[successor] = scopes
                        successor_scope_map[successor] = scopes
                    else:
                        self.scope_map[successor] = relevant_scopes
                        successor_scope_map[successor] = relevant_scopes

            qualified_nodes = [successor for successor in successors if self.in_degrees[successor] == 0]
        tasks = [
            self.execute_node(successor, scope)
            for successor in qualified_nodes
            for scope in successor_scope_map[successor]
        ]
        await asyncio.gather(*tasks)

    async def node_task(self, node: Node, scope: Scope, args: Dict[str, Any]) -> None:
        result = await node(**args)
        scopes = set()
        if not node.loop_vars:
            scope[node] = result
            if not isinstance(result, IgnoreResult):
                scopes.add(scope)
        else:
            child = scope.birth()
            child[node] = result
            if not isinstance(result, IgnoreResult):
                scopes.add(child)

        await self.execute_successors(node, scopes)

    async def execute_node(self, node: Node, scope: Scope) -> None:
        args_list = await self._resolve_args(node, scope)

        task = asyncio.gather(*[node(**single_args) for single_args in args_list])
        if self.pbar:
            with self.update_pbar(len(args_list)):
                results = await task
        else:
            results = await task

        scopes = set()
        if not node.loop_vars:
            scope[node] = results[0]
            if not isinstance(results[0], IgnoreResult):
                scopes.add(scope)
        else:
            scope = scope.birth()
            scope["scope_name"] = str(node)
            for result in results:
                child = scope.birth()
                child[node] = result
                if not isinstance(result, IgnoreResult):
                    scopes.add(child)

        await self.execute_successors(node, scopes)

    def create_flow_graph(self, *target_nodes) -> MultiDiGraph:
        flow_graph: MultiDiGraph = self.graph.copy()
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

    def construct_mutex_map(self, work_graph: MultiDiGraph) -> None:
        for node in work_graph:
            # get the predecessors of the successors without cur node
            predecessors_2nd_order = set()
            for successor in work_graph.successors(node):
                predecessors_2nd_order.update(work_graph.predecessors(successor))
            predecessors_2nd_order.discard(node)

            if len(predecessors_2nd_order) > 1:
                self.second_order_predecessor_map[node] = predecessors_2nd_order

        self.mutex_map = {
            node: asyncio.Lock()
            for node in work_graph
        }

    async def execute(self, *target_nodes: Sequence[Node], with_pbar: Optional[bool] = True) -> Scope:
        """Execute the graph layer by layer."""
        self.flow_graph = self.create_flow_graph(*target_nodes)
        root = Scope()
        self.construct_mutex_map(self.flow_graph)
        self.scope_map = {}

        self.in_degrees = {node: self.flow_graph.in_degree(node) for node in self.flow_graph}

        qualified_nodes = [node for node in self.in_degrees.keys() if self.in_degrees[node] == 0]

        if with_pbar:
            with tqdm(total=len(qualified_nodes), desc="Executing Deppy Graph", unit="node") as self.pbar:
                await asyncio.gather(*[self.execute_node(node, root) for node in qualified_nodes])
                self.pbar.update(len(qualified_nodes))
            self.pbar = None
        else:
            await asyncio.gather(*[self.execute_node(node, root) for node in qualified_nodes])
        return root

    async def _resolve_args(self, node: Node, scope: Scope) -> List[Dict[str, Any]]:
        resolved_args = {
            key: scope[pred]
            for pred, _,  key in self.graph.in_edges(node, keys=True)
        }

        if node.loop_vars:
            loop_keys = [key for key, _ in node.loop_vars]
            loop_values = (resolved_args[key] for key in loop_keys)
            return [
                {**resolved_args, **dict(zip(loop_keys, combination))}
                for combination in node.loop_strategy(*loop_values)
            ]

        return [resolved_args]
