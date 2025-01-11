import asyncio
from itertools import product
from typing import Optional

from .node import Node


class ScopedDict(dict):
    def __init__(self, parent: Optional[dict] = None):
        self.parent = parent
        self.children = []
        super().__init__()

    def __call__(self, key):
        values = []
        val = self.get(key)
        if val:
            values.append(val)
        for child in self.children:
            values.extend(child(key))
        return values

    def __getitem__(self, item):
        val = self.get(item)
        if val is not None:
            return val
        if self.parent is not None:
            return self.parent[item]
        raise KeyError(item)

    def dump(self, str_keys=False):
        if str_keys:
            cp = {str(k): v for k, v in self.items()}
        else:
            cp = self.copy()
        if len(self.children) > 0:
            cp["children"] = [child.dump(str_keys=str_keys) for child in self.children]
        return cp

    def __str__(self):
        return str(self.dump())

    def birth(self):
        child = ScopedDict(self)
        self.children.append(child)
        return child

    def __hash__(self):
        return id(self)


class Executor:
    """Executes a dependency graph in layers."""

    def __init__(self, graph):
        self.graph = graph
        self.root = ScopedDict()
        self.work_graph = self.graph.copy()

    async def execute_node(self, node, scope):
        args_list = await self._resolve_args(node, scope)
        results = await asyncio.gather(*[node(**single_args) for single_args in args_list])

        scopes = set()
        if not node.loop_vars:
            scope[node] = results[0]
            scopes.add(scope)
        else:
            child = scope.birth()
            child["scope_name"] = str(node)
            for result in results:
                new_child = child.birth()
                new_child[node] = result
                scopes.add(new_child)

        successors = self.graph.successors(node)
        if node in self.work_graph:
            self.work_graph.remove_node(node)
        qualified_nodes = [successor for successor in successors if self.work_graph.in_degree(successor) == 0]
        await asyncio.gather(*[self.execute_node(successor, scope) for successor in qualified_nodes for scope in scopes])

    async def execute(self) -> ScopedDict:
        """Execute the graph layer by layer."""
        self.work_graph = self.graph.copy()
        qualified_nodes = [node for node in self.work_graph if self.work_graph.in_degree(node) == 0]
        await asyncio.gather(*[self.execute_node(node, self.root) for node in qualified_nodes])
        return self.root

    @staticmethod
    async def _resolve_args(node, scope):
        resolved_args = {}

        for name, dep in node.dependencies.items():
            if isinstance(dep, Node):
                resolved_args[name] = scope[dep]
            elif callable(dep):
                resolved_args[name] = await asyncio.to_thread(dep)
            else:
                resolved_args[name] = dep

        # Handle Cartesian product for loop variables
        if node.loop_vars:
            loop_keys = [name for name, _ in node.loop_vars]
            loop_values = [scope[dep] if isinstance(dep, Node) else dep for _, dep in node.loop_vars]
            return [{**resolved_args, **dict(zip(loop_keys, combination))} for combination in product(*loop_values)]

        return [resolved_args]
