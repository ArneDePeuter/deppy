import time
import json
from typing import Dict, Any

from .call_strategy import CallStrategy


class Cache(CallStrategy, dict):
    def __init__(self, max_uses=None, ttl=None):
        assert max_uses is None or max_uses > 0, "max_uses must be a positive integer or None"
        self.max_uses = max_uses
        self.ttl = ttl
        self.usage_count = {} if max_uses else None
        self.ttls = {} if ttl else None
        super().__init__()

    async def __call__(self, func, **kwargs):
        cache_key = self.create_cache_key(kwargs)
        value, hit = self.get(cache_key)
        if hit:
            return value
        res = await func(**kwargs)
        self.set(cache_key, res)
        return res

    @staticmethod
    def create_cache_key(args: Dict[str, Any]) -> str:
        try:
            return json.dumps(args, sort_keys=True, default=str)
        except TypeError:
            return str(sorted(args.items()))

    def get(self, key):
        if key not in self:
            return None, False

        # Check TTL
        if self.ttls:
            timestamp = self.ttls.get(key, 0)
            if self.ttl and (time.time() - timestamp > self.ttl):
                self.invalidate(key)
                return None, False

        # Check max uses
        if self.usage_count:
            self.usage_count[key] += 1
            if self.usage_count[key] > self.max_uses:
                self.invalidate(key)
                return None, False

        return self[key], True

    def set(self, key, value):
        """Store a value in the cache."""
        self[key] = value
        if self.usage_count is not None:
            self.usage_count[key] = 0
        if self.ttls is not None:
            self.ttls[key] = time.time()

    def invalidate(self, key):
        """Invalidate a cache entry."""
        if key in self:
            del self[key]
        if self.usage_count and key in self.usage_count:
            del self.usage_count[key]
        if self.ttls and key in self.ttls:
            del self.ttls[key]
