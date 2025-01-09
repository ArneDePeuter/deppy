import time
from deppy.cache import Cache


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

    cache.get("key1")
