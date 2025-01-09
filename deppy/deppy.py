from inspect import signature

from .node import Node


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

    @staticmethod
    def resolve_dependency(dependency):
        """Resolve a dependency by calling the associated function or executing the Node."""
        if isinstance(dependency, Node):
            return dependency.execute()  # Resolve Node by executing it
        elif callable(dependency):  # Handle direct function references
            return dependency()
        return dependency  # Handle static values

    def call_with_dependencies(self, func, additional_kwargs=None):
        """Call a function with its dependencies automatically resolved."""
        additional_kwargs = additional_kwargs or {}

        sig = signature(func)
        resolved_args = {}

        # Resolve each parameter
        for param_name, param in sig.parameters.items():
            if param_name in additional_kwargs:
                resolved_args[param_name] = additional_kwargs[param_name]
            elif param.default is not param.empty:
                resolved_args[param_name] = self.resolve_dependency(param.default)
            else:
                raise ValueError(f"Missing dependency for parameter: {param_name}")

        return func(**resolved_args)
