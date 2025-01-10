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

    def __init__(self, nodes):
        self.nodes = nodes
        self.layers = self._build_layers()

    def _build_layers(self):
        """Build layers of independent nodes."""
        layers = []
        visited = set()
        dependency_map = {node: list(node.dependencies.values()) for node in self.nodes}

        while dependency_map:
            # Identify nodes with no dependencies (ready to execute)
            ready = [node for node, deps in dependency_map.items() if all(dep in visited for dep in deps)]

            if not ready:
                raise ValueError("Circular dependency detected in the graph!")

            layers.append(ready)
            visited.update(ready)

            # Remove resolved nodes
            for node in ready:
                del dependency_map[node]

        return layers

    async def execute_node(self, node, results: ScopedDict, func_task_cache: dict):
        args_list = await self._resolve_args(node, results)
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

        birthed_children = set()

        if not node.loop_vars:
            results[node] = node_tasks[0].result()
            return birthed_children

        child = results.birth()
        child["scope_name"] = str(node)
        for task in node_tasks:
            result = task.result()
            new_child = child.birth()
            new_child[node] = result
            birthed_children.add(new_child)
        return birthed_children

    async def execute_layer(self, layer, parent_children: dict[Node, set[ScopedDict]], root: ScopedDict):
        tasks = []
        task_map = {}
        func_task_cache = {}

        for node in layer:
            node_tasks = []

            dependencies = [dep for dep in node.dependencies.values() if isinstance(dep, Node)]
            relevant_scopes = {dep: parent_children[dep] for dep in dependencies if dep in parent_children}
            assert len(relevant_scopes) < 2, f"Joining scopes not implemented, detected in {node}"

            scopes = next(iter(relevant_scopes.values())) if len(relevant_scopes) > 0 else [root]

            new_tasks = [asyncio.create_task(self.execute_node(node, scope, func_task_cache)) for scope in scopes]
            tasks.extend(new_tasks)
            node_tasks.extend(new_tasks)
            task_map[node] = node_tasks

        await asyncio.gather(*tasks)

        for node, node_tasks in task_map.items():
            new_children = set()
            for task in node_tasks:
                new_children |= task.result()
            if len(new_children) > 0:
                parent_children[node] = new_children
        return parent_children

    async def execute(self) -> ScopedDict:
        """Execute the graph layer by layer."""
        root = ScopedDict()
        parent_children = {}

        for layer in self.layers:
            parent_children = await self.execute_layer(layer, parent_children, root)

        return root

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
