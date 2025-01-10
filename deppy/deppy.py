from .node import Node
from .executor import Executor


class Deppy:
    """Main dependency resolver."""

    def __init__(self):
        self.functions = {}

    def node(self, func=None, cache=None):
        """Register a function as a node with optional caching."""
        def decorator(f):
            wrapper = Node(f, self, cache=cache)
            self.functions[f.__name__] = wrapper
            return wrapper

        if func:
            return decorator(func)
        return decorator

    async def execute(self):
        """Execute the dependency graph."""
        executor = Executor(list(self.functions.values()))
        return await executor.execute()

    def executor(self):
        return Executor(list(self.functions.values()))

    def dot(self, filename: str="graph.dot"):
        """Generate a graphviz dot file."""
        import pydot
        graph = pydot.Dot("my_graph", graph_type="digraph")
        for node in self.functions.values():
            graph.add_node(pydot.Node(str(node)))
            for name, dep in node.dependencies.items():
                label = f"return->{name}"
                if (name, dep) in node.loop_vars:
                    label = label.replace("return", "loop[return]")
                    graph.add_edge(pydot.Edge(str(dep), str(node), label=label, color="red"))
                else:
                    graph.add_edge(pydot.Edge(str(dep), str(node), label=label))
        graph.write(filename)
