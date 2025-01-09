import json
from itertools import product


class Node:
    """A wrapper to manage function nodes and dependencies."""

    def __init__(self, func, deppy, cache=None):
        self.func = func
        self.deppy = deppy
        self.dependencies = {}  # Store dependencies for arguments
        self.loop_vars = []  # Store loop variables as (arg_name, iterable)
        self.cache = cache

    def __getattr__(self, name):
        """Allow dynamic setting of dependencies."""

        def setter(dependency, loop=False):
            if loop:
                self.loop_vars.append((name, dependency))
            else:
                self.dependencies[name] = dependency
            return self  # Allow chaining

        return setter

    def execute(self):
        """Execute the wrapped function with resolved dependencies."""
        # Resolve all dependencies
        resolved_args = {
            name: self.deppy.resolve_dependency(dep)
            for name, dep in self.dependencies.items()
        }

        # Compute Cartesian product of loop variables
        if self.loop_vars:
            loop_keys = [name for name, _ in self.loop_vars]
            loop_values = [self.deppy.resolve_dependency(dep) for _, dep in self.loop_vars]
            cartesian_product = product(*loop_values)

            results = []
            for combination in cartesian_product:
                loop_args = dict(zip(loop_keys, combination))
                args = {**resolved_args, **loop_args}
                results.append(self.call_with_cache(args))
            return results
        else:
            # No loops, just call the function directly
            return self.call_with_cache(resolved_args)

    def call_with_cache(self, args):
        """Call the function with caching support."""
        if self.cache:
            # Create a robust cache key
            cache_key = self.create_cache_key(args)
            cached_result, hit = self.cache.get(cache_key)
            if hit:
                return cached_result

            # Compute the result and cache it
            result = self.deppy.call_with_dependencies(self.func, args)
            self.cache.set(cache_key, result)
            return result
        else:
            # No cache, just call the function
            return self.deppy.call_with_dependencies(self.func, args)

    @staticmethod
    def create_cache_key(args):
        """Create a robust and unique cache key."""
        try:
            return json.dumps(args, sort_keys=True, default=str)
        except TypeError:
            # Fallback for non-serializable objects
            return str(sorted(args.items()))
