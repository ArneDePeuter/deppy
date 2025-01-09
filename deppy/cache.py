import time


class Cache:
    """Cache class to handle TTL and max_uses."""

    def __init__(self, max_uses=None, ttl=None):
        assert max_uses > 0 if max_uses else True, "max_uses must be a positive integer"
        self.max_uses = max_uses
        self.ttl = ttl
        self.cache = {}
        self.usage_count = {}
        self.ttls = {}

    def get(self, key):
        """Retrieve a value from the cache if valid."""
        if key not in self.cache:
            return None, False

        # Check TTL
        if self.ttl and time.time() - self.ttls[key] > self.ttl:
            self.invalidate(key)
            return None, False

        # Check max uses
        if self.max_uses:
            self.usage_count[key] += 1
            if self.usage_count[key] > self.max_uses:
                self.invalidate(key)
                return None, False

        return self.cache[key], True

    def set(self, key, value):
        """Store a value in the cache."""
        self.cache[key] = value
        if self.max_uses:
            self.usage_count[key] = 0
        if self.ttl:
            self.ttls[key] = time.time()

    def invalidate(self, key):
        """Invalidate a cache entry."""
        if key in self.cache:
            del self.cache[key]
            if self.max_uses:
                del self.usage_count[key]
            if self.ttl:
                del self.ttls[key]
