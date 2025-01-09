from deppy.node import Node
from deppy.cache import Cache
from deppy.deppy import Deppy


def test_node_execution_without_dependencies():
    deppy = Deppy()

    @deppy.node
    def test_node():
        return "result"

    assert test_node.execute() == "result"


def test_node_with_dependencies():
    deppy = Deppy()

    @deppy.node
    def dependency():
        return "dependency_result"

    @deppy.node
    def test_node(dep=dependency):
        return f"node_result: {dep}"

    assert test_node.execute() == "node_result: dependency_result"


def test_node_with_loop_variables():
    deppy = Deppy()

    @deppy.node
    def loop1():
        return [1, 2]

    @deppy.node
    def loop2():
        return ["a", "b"]

    @deppy.node
    def test_node(val1=loop1, val2=loop2):
        return f"{val1}-{val2}"

    test_node.val1(loop1, loop=True)
    test_node.val2(loop2, loop=True)

    assert test_node.execute() == ["1-a", "1-b", "2-a", "2-b"]


def test_node_with_cache():
    deppy = Deppy()
    cache = Cache()

    @deppy.node(cache=cache)
    def cached_node():
        return "cached_result"

    assert cached_node.execute() == "cached_result"
    result, hit = cache.get(Node.create_cache_key({}))
    assert hit is True
    assert result == "cached_result"
