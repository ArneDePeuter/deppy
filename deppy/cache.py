import time


class Cache:
    """Cache class to handle TTL and max_uses."""

    def __init__(self, max_uses=None, ttl=None):
        assert max_uses is None or max_uses > 0, "max_uses must be a positive integer or None"
        self.max_uses = max_uses
        self.ttl = ttl
        self.cache = {}
        self.usage_count = {} if max_uses else None
        self.ttls = {} if ttl else None

    def get(self, key):
        """Retrieve a value from the cache if valid."""
        if key not in self.cache:
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

        return self.cache[key], True

    def set(self, key, value):
        """Store a value in the cache."""
        self.cache[key] = value
        if self.usage_count is not None:
            self.usage_count[key] = 0
        if self.ttls is not None:
            self.ttls[key] = time.time()

    def invalidate(self, key):
        """Invalidate a cache entry."""
        if key in self.cache:
            del self.cache[key]
        if self.usage_count and key in self.usage_count:
            del self.usage_count[key]
        if self.ttls and key in self.ttls:
            del self.ttls[key]
