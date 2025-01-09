import asyncio
from itertools import product

from .node import Node


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

    async def execute(self):
        """Execute the graph layer by layer."""
        results = {}

        for layer in self.layers:
            tasks = {}
            func_task_cache = {}

            for node in layer:
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

                tasks[node] = node_tasks

            await asyncio.gather(*[task for tasks in tasks.values() for task in tasks])

            for node, node_tasks in tasks.items():
                results[node] = [task.result() for task in node_tasks] if node.loop_vars else node_tasks[0].result()

        return {node.func.__name__: results[node] for node in self.nodes}

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
