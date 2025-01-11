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
        self.scope_map = {}

    async def execute_node_in_scope(self, node, scope, func_task_cache: dict):
        args_list = await self._resolve_args(node, scope)
        node_tasks = []

        for single_args in args_list:
            func_task_key = (node.func.__name__, node.create_cache_key(single_args))
            existing_task = func_task_cache.get(func_task_key)
            if existing_task is None:
                task = asyncio.create_task(node(**single_args))
                func_task_cache[func_task_key] = task
                node_tasks.append(task)
            else:
                node_tasks.append(existing_task)

        await asyncio.gather(*node_tasks)

        if not node.loop_vars:
            scope[node] = node_tasks[0].result()
            return

        child = scope.birth()
        child["scope_name"] = str(node)
        if node not in self.scope_map:
            self.scope_map[node] = set()
        for task in node_tasks:
            result = task.result()
            new_child = child.birth()
            new_child[node] = result
            self.scope_map[node].add(new_child)

    async def execute_node(self, node, func_task_cache: dict):
        predecessors = self.graph.predecessors(node)
        scopes = [self.scope_map[predecessor] for predecessor in predecessors if predecessor in self.scope_map]
        assert len(scopes) < 2, f"Joining scopes not implemented, detected in {node}"

        scopes = scopes[0] if scopes else [self.root]

        await asyncio.gather(*[self.execute_node_in_scope(node, scope, func_task_cache) for scope in scopes])

    async def execute(self) -> ScopedDict:
        """Execute the graph layer by layer."""
        self.work_graph = self.graph.copy()
        while qualified_nodes := [node for node in self.work_graph if self.work_graph.in_degree(node) == 0]:
            func_task_cache = {}
            node_tasks = [self.execute_node(node, func_task_cache) for node in qualified_nodes]
            await asyncio.gather(*node_tasks)
            for node in qualified_nodes:
                self.work_graph.remove_node(node)
        return self.root

    @staticmethod
    async def _resolve_args(node, results):
        """Resolve arguments for a node based on resolved dependencies."""
        resolved_args = {}

        for name, dep in node.dependencies.items():
            if isinstance(dep, Node):
                resolved_args[name] = results[dep]
            elif callable(dep):
                resolved_args[name] = await asyncio.to_thread(dep)
            else:
                resolved_args[name] = dep

        # Handle Cartesian product for loop variables
        if node.loop_vars:
            loop_keys = [name for name, _ in node.loop_vars]
            loop_values = [results[dep] if isinstance(dep, Node) else dep for _, dep in node.loop_vars]
            return [{**resolved_args, **dict(zip(loop_keys, combination))} for combination in product(*loop_values)]

        return [resolved_args]
