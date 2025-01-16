from deppy import Deppy
import asyncio


def test_get_node_by_name():
    def func_1():
        return 1

    deppy = Deppy()
    node = deppy.add_node(func_1, name="func_1")

    assert deppy.get_node_by_name("func_1") == node
    assert deppy.get_node_by_name("func_2") is None


def test_execute_is_async():
    deppy = Deppy()

    def func_1():
        return 1

    deppy.add_node(func_1)

    assert deppy.execute_is_async() is False

    async def async_func_1():
        return 1

    deppy.add_node(async_func_1)

    assert deppy.execute_is_async() is True


def test_execute():
    deppy = Deppy()

    def func_1():
        return 1

    deppy.add_node(func_1)

    exec_func = deppy.execute
    assert not asyncio.iscoroutinefunction(exec_func)
    assert exec_func.__name__ == "execute_sync"

    async def async_func_1():
        return 1

    deppy.add_node(async_func_1)

    exec_func = deppy.execute
    assert asyncio.iscoroutinefunction(exec_func)
    assert exec_func.__name__ == "execute_hybrid"
