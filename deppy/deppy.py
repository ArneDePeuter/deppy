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
