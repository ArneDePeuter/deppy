import json
import asyncio


class Node:
    """A wrapper to manage function nodes and dependencies."""

    def __init__(self, func, deppy, loop_method, cache):
        self.func = func
        self.deppy = deppy
        self.dependencies = {}  # Store dependencies for arguments
        self.loop_vars = []  # Store loop variables as (arg_name, iterable)
        self.cache = cache
        self.loop_method = loop_method

    async def __call__(self, **kwargs):
        if self.cache:
            cache_key = self.create_cache_key(kwargs)
            value, hit = self.cache.get(cache_key)
            if hit:
                return value

        if asyncio.iscoroutinefunction(self.func):
            res = await self.func(**kwargs)
        else:
            res = self.func(**kwargs)

        if self.cache:
            self.cache.set(cache_key, res)

        return res

    def __repr__(self):
        return f"<Node {self.func.__name__}>"

    def __str__(self):
        return self.func.__name__

    def __getattr__(self, name):
        """Allow dynamic setting of dependencies."""
        def setter(dependency, loop=False):
            self.deppy.edge(dependency, self, name, loop=loop)
            self.dependencies[name] = dependency
            return self
        return setter

    @staticmethod
    def create_cache_key(args):
        """Create a robust and unique cache key."""
        try:
            return json.dumps(args, sort_keys=True, default=str)
        except TypeError:
            return str(sorted(args.items()))
