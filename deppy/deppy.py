from .node import Node
from .executor import Executor
from networkx import MultiDiGraph, is_directed_acyclic_graph


class Deppy:
    """Main dependency resolver."""

    def __init__(self):
        self.graph = MultiDiGraph()
        self.executor = Executor(self.graph)

    def node(self, func=None, cache=None):
        """Register a function as a node with optional caching."""
        def decorator(f):
            node = Node(f, self, cache=cache)
            self.graph.add_node(node)
            assert is_directed_acyclic_graph(self.graph), "Circular dependency detected in the graph!"
            return node

        if func:
            return decorator(func)
        return decorator

    def edge(self, node1, node2, in_kwarg_name, loop=False):
        if loop:
            node2.loop_vars.append((in_kwarg_name, node1))
        self.graph.add_edge(node1, node2, key=in_kwarg_name, loop=loop)
        assert is_directed_acyclic_graph(self.graph), "Circular dependency detected in the graph!"

    async def execute(self):
        """Execute the dependency graph."""
        return await self.executor.execute()

