import pytest
from deppy.node import Node, NodeFunctionError


def test_is_async():
    async def async_func():
        pass

    node_async = Node(func=async_func)
    assert node_async.is_async is True

    def sync_func():
        pass

    node_sync = Node(func=sync_func)
    assert node_sync.is_async is False


def test_call_sync():
    def sample_func(x, y):
        return x + y

    node = Node(func=sample_func)

    result = node.call_sync(3, 4)
    assert result == 7

    def error_func(x):
        raise ValueError("Test error")

    node = Node(func=error_func)
    with pytest.raises(NodeFunctionError) as exc_info:
        node.call_sync(1)
    assert "Error executing node function of" in str(exc_info.value)


@pytest.mark.asyncio
async def test_call_async():
    async def async_func(x):
        return x * 2

    node = Node(func=async_func)
    result = await node.call_async(5)
    assert result == 10

    async def async_error_func(x):
        raise ValueError("Async error")

    node = Node(func=async_error_func)
    with pytest.raises(NodeFunctionError) as exc_info:
        await node.call_async(5)
    assert "Error executing node function of" in str(exc_info.value)


def test_repr():
    def sample_func():
        pass

    node = Node(func=sample_func, name="TestNode")
    assert repr(node) == "<Node TestNode>"


def test_str():
    def sample_func():
        pass

    node = Node(func=sample_func, name="TestNode")
    assert str(node) == "TestNode"
