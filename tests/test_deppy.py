import asyncio
from deppy import Deppy
from itertools import product


def test_deppy_register_node():
    deppy = Deppy()

    @deppy.node
    def test_node():
        return "node_registered"

    assert test_node in deppy.graph
    assert asyncio.run(deppy.execute()) == {test_node: "node_registered"}


async def test_deppy_execute_graph():
    deppy = Deppy()

    @deppy.node
    def node1():
        return "node1_result"

    @deppy.node
    def node2(dep):
        return f"node2_result: {dep}"

    node2.dep(node1)

    result = await deppy.execute()
    assert result == {node1: "node1_result", node2: "node2_result: node1_result"}


async def test_unique_scope_upon_loop():
    def l():
        return [1, 2, 3]

    def item1(data):
        return data * 2

    def item2(data):
        return data * 3

    def item3(data1, data2):
        return data1, data2

    deppy = Deppy()

    l_node = deppy.node(l)
    item1_node = deppy.node(item1)
    item2_node = deppy.node(item2)
    item3_node = deppy.node(item3)

    item1_node.data(l_node, loop=True)
    item2_node.data(item1_node)
    item3_node.data1(item1_node)
    item3_node.data2(item2_node)

    result = await deppy.execute()

    assert result(item3_node) == [(2, 6), (4, 12), (6, 18)]


async def test_loopmethod_zip():
    def l1():
        return [1, 2, 3]

    def l2():
        return ["a", "b", "c"]

    def item1(data1, data2):
        return data1, data2

    deppy = Deppy()

    l1_node = deppy.node(l1)
    l2_node = deppy.node(l2)
    item1_node = deppy.node(item1, loop_strategy=zip)

    item1_node.data1(l1_node, loop=True).data2(l2_node, loop=True)

    result = await deppy.execute()
    assert result(item1_node) == [(1, "a"), (2, "b"), (3, "c")]


async def test_loopmethod_cartesian():
    def l1():
        return [1, 2, 3]

    def l2():
        return ["a", "b", "c"]

    def item1(data1, data2):
        return data1, data2

    deppy = Deppy()

    l1_node = deppy.node(l1)
    l2_node = deppy.node(l2)
    item1_node = deppy.node(item1, loop_strategy=product)

    item1_node.data1(l1_node, loop=True).data2(l2_node, loop=True)

    result = await deppy.execute()
    assert result(item1_node) == [(1, "a"), (1, "b"), (1, "c"), (2, "a"), (2, "b"), (2, "c"), (3, "a"), (3, "b"), (3, "c")]


async def test_extractor():
    deppy = Deppy()

    async def lists():
        return [1, 2], ["a", "b"]

    async def combine(val1, val2):
        return f"{val1}-{val2}"

    lists_node = deppy.node(lists)
    combine_node = deppy.node(combine, loop_strategy=zip)

    combine_node.val1(lists_node, loop=True, extractor=lambda x: x[0]).val2(lists_node, loop=True, extractor=lambda x: x[1])

    result = await deppy.execute()
    assert result(combine_node) == ["1-a", "2-b"]
    assert result(lists_node) == [([1, 2], ["a", "b"])]

    assert len(result.children[0].children) == 2
