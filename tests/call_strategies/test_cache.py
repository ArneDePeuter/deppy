import time
from deppy.call_strategies.cache import Cache
from deppy.deppy import Deppy


def test_cache_set_and_get():
    cache = Cache()
    cache.set("key1", "value1")
    result, hit = cache.get("key1")
    assert hit is True
    assert result == "value1"


def test_cache_ttl_expiry():
    cache = Cache(ttl=1)  # 1-second TTL
    cache.set("key1", "value1")
    time.sleep(1.1)
    result, hit = cache.get("key1")
    assert hit is False
    assert result is None


def test_cache_max_uses():
    cache = Cache(max_uses=2)
    cache.set("key1", "value1")
    result, hit = cache.get("key1")
    assert hit is True
    assert result == "value1"

    # Second access
    cache.get("key1")
    # Third access should invalidate
    result, hit = cache.get("key1")
    assert hit is False
    assert result is None


async def test_cache_integration_with_node():
    cache = Cache(ttl=2)

    async def function():
        return "cached_result"

    # First execution
    result = await cache(function)
    assert result == "cached_result"

    # Validate cache hit
    cache_key = cache.create_cache_key({})
    cached_result, hit = cache.get(cache_key)
    assert hit is True
    assert cached_result == "cached_result"

    # Wait for TTL to expire
    time.sleep(2.1)
    cached_result, hit = cache.get(cache_key)
    assert hit is False
    assert cached_result is None
