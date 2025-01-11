import asyncio
from deppy import Deppy, IgnoreResult
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


async def test_solo_race():
    async def list1():
        return [1, 2]

    async def process(data):
        await asyncio.sleep(data)
        return data * 2

    entry_times = []

    async def process_again(data):
        import time
        entry_times.append(time.time())
        return data * 3

    deppy = Deppy()

    list1_node = deppy.node(list1)
    process_node = deppy.node(process, team_race=False)
    process_again_node = deppy.node(process_again)

    process_node.data(list1_node, loop=True)
    process_again_node.data(process_node)

    result = await deppy.execute()

    assert result(process_again_node) == [6, 12]

    diff = entry_times[1] - entry_times[0]
    # branch diff is greater than 3 meaning one process started earlier than the other
    assert abs(diff - 1) < 0.1

    process_node.team_race = True

    entry_times.clear()
    result = await deppy.execute()

    assert result(process_again_node) == [6, 12]

    diff = entry_times[1] - entry_times[0]
    # processes started at the same time
    assert diff < 0.1


async def test_node_execution_without_dependencies():
    deppy = Deppy()

    @deppy.node
    async def test_node():
        return "result"

    result = await deppy.execute()
    assert result == {test_node: "result"}


async def test_node_with_dependencies():
    deppy = Deppy()

    @deppy.node
    async def dependency():
        return "dependency_result"

    @deppy.node
    async def test_node(dep):
        return f"node_result: {dep}"

    test_node.dep(dependency)

    result = await deppy.execute()
    assert result == {dependency: "dependency_result", test_node: "node_result: dependency_result"}


async def test_node_with_loop_variables():
    deppy = Deppy()

    @deppy.node
    async def loop1():
        return [1, 2]

    @deppy.node
    async def loop2():
        return ["a", "b"]

    @deppy.node
    async def test_node(val1, val2):
        return f"{val1}-{val2}"

    test_node.val1(loop1, loop=True)
    test_node.val2(loop2, loop=True)

    result = await deppy.execute()
    assert result(test_node) == ["1-a", "1-b", "2-a", "2-b"]
    assert result(loop1) == [[1, 2]]
    assert result(loop2) == [["a", "b"]]

    assert len(result.children[0].children) == 4


async def test_ignore_result():
    deppy = Deppy()

    async def l():
        return [2, 4, 3]

    def filter_uneven(data):
        return IgnoreResult(data=data) if data % 2 != 0 else data

    def increment(data):
        return data + 1

    l_node = deppy.node(l)
    filter_node = deppy.node(filter_uneven)
    increment_node = deppy.node(increment)

    filter_node.data(l_node, loop=True)
    increment_node.data(filter_node)

    result = await deppy.execute()
    assert result(increment_node) == [3, 5]
    all_filter_results = result(filter_node)
    assert len(all_filter_results) == 3
    all_filter_valid_results = result(filter_node, ignored_results=False)
    assert len(all_filter_valid_results) == 2
    all_filter_invalid_results = result(filter_node, ignored_results=True)
    assert len(all_filter_invalid_results) == 1
    assert all_filter_invalid_results[0].data == 3


async def test_constant():
    deppy = Deppy()

    async def add(val1, val2):
        return val1 + val2

    l1 = deppy.const([1, 2, 3])
    l2 = deppy.const([1, 2, 3])
    add_node = deppy.node(add, loop_strategy=zip)

    add_node.val1(l1, loop=True).val2(l2, loop=True)

    result = await deppy.execute()
    assert result(add_node) == [2, 4, 6]
