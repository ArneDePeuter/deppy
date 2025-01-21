from deppy import Deppy, IgnoreResult
from itertools import product


def test_deppy_register_node():
    deppy = Deppy()

    def test_node():
        return "node_registered"

    test_node = deppy.add_node(test_node)

    assert test_node in deppy.graph
    assert deppy.execute() == {test_node: "node_registered"}


async def test_deppy_execute_graph():
    deppy = Deppy()

    def node1():
        return "node1_result"

    def node2(dep):
        return f"node2_result: {dep}"

    node1 = deppy.add_node(node1)
    node2 = deppy.add_node(node2)
    deppy.add_edge(node1, node2, "dep")

    result = deppy.execute()
    assert result == {node1: "node1_result", node2: "node2_result: node1_result"}


async def test_unique_scope_upon_loop():
    def my_list():
        return [1, 2, 3]

    def item1(data):
        return data * 2

    def item2(data):
        return data * 3

    def item3(data1, data2):
        return data1, data2

    deppy = Deppy()

    l_node = deppy.add_node(my_list)
    item1_node = deppy.add_node(item1)
    item2_node = deppy.add_node(item2)
    item3_node = deppy.add_node(item3)

    deppy.add_edge(l_node, item1_node, "data", loop=True)
    deppy.add_edge(item1_node, item2_node, "data")
    deppy.add_edge(item1_node, item3_node, "data1")
    deppy.add_edge(item2_node, item3_node, "data2")

    result = deppy.execute()

    assert result.query(item3_node) == [(2, 6), (4, 12), (6, 18)]


async def test_loopmethod_zip():
    def l1():
        return [1, 2, 3]

    def l2():
        return ["a", "b", "c"]

    def item1(data1, data2):
        return data1, data2

    deppy = Deppy()

    l1_node = deppy.add_node(l1)
    l2_node = deppy.add_node(l2)
    item1_node = deppy.add_node(item1, loop_strategy=zip)

    deppy.add_edge(l1_node, item1_node, "data1", loop=True)
    deppy.add_edge(l2_node, item1_node, "data2", loop=True)

    result = deppy.execute()
    assert result.query(item1_node) == [(1, "a"), (2, "b"), (3, "c")]


async def test_loopmethod_cartesian():
    def l1():
        return [1, 2, 3]

    def l2():
        return ["a", "b", "c"]

    def item1(data1, data2):
        return data1, data2

    deppy = Deppy()

    l1_node = deppy.add_node(l1)
    l2_node = deppy.add_node(l2)
    item1_node = deppy.add_node(item1, loop_strategy=product)

    deppy.add_edge(l1_node, item1_node, "data1", loop=True)
    deppy.add_edge(l2_node, item1_node, "data2", loop=True)

    result = deppy.execute()
    assert result.query(item1_node) == [
        (1, "a"),
        (1, "b"),
        (1, "c"),
        (2, "a"),
        (2, "b"),
        (2, "c"),
        (3, "a"),
        (3, "b"),
        (3, "c"),
    ]


async def test_output():
    deppy = Deppy()

    async def lists():
        return [1, 2], ["a", "b"]

    async def combine(val1, val2):
        return f"{val1}-{val2}"

    lists_node = deppy.add_node(lists)
    lists_node_output_1 = deppy.add_output(lists_node, "val1", extractor=lambda x: x[0])
    lists_node_output_2 = deppy.add_output(lists_node, "val2", extractor=lambda x: x[1])
    combine_node = deppy.add_node(combine, loop_strategy=zip)

    deppy.add_edge(lists_node_output_1, combine_node, "val1", loop=True)
    deppy.add_edge(lists_node_output_2, combine_node, "val2", loop=True)

    result = await deppy.execute()
    assert result.query(combine_node) == ["1-a", "2-b"]
    assert result.query(lists_node) == [([1, 2], ["a", "b"])]

    assert len(result.children) == 1
    assert len(result.children[0].children) == 2


async def test_node_execution_without_dependencies():
    deppy = Deppy()

    async def test():
        return "result"

    test_node = deppy.add_node(test)

    result = await deppy.execute()
    assert result == {test_node: "result"}


async def test_node_with_dependencies():
    deppy = Deppy()

    async def dependency():
        return "dependency_result"

    async def test(dep):
        return f"node_result: {dep}"

    dependency_node = deppy.add_node(dependency)
    test_node = deppy.add_node(test)

    deppy.add_edge(dependency_node, test_node, "dep")

    result = await deppy.execute()
    assert result == {
        dependency_node: "dependency_result",
        test_node: "node_result: dependency_result",
    }


async def test_ignore_result():
    async def my_list():
        return [2, 4, 3]

    def filter_uneven(data):
        return IgnoreResult(data=data) if data % 2 != 0 else data

    def increment(data):
        return data + 1

    deppy = Deppy()

    l_node = deppy.add_node(my_list)
    filter_node = deppy.add_node(filter_uneven)
    increment_node = deppy.add_node(increment)

    deppy.add_edge(l_node, filter_node, "data", loop=True)
    deppy.add_edge(filter_node, increment_node, "data")

    result = await deppy.execute()
    assert result.query(increment_node) == [3, 5]
    all_filter_results = result.query(filter_node)
    assert len(all_filter_results) == 3
    all_filter_valid_results = result.query(filter_node, ignored_results=False)
    assert len(all_filter_valid_results) == 2
    all_filter_invalid_results = result.query(filter_node, ignored_results=True)
    assert len(all_filter_invalid_results) == 1
    assert all_filter_invalid_results[0].data == 3


async def test_constant():
    async def add(val1, val2):
        return val1 + val2

    deppy = Deppy()

    l1 = deppy.add_const([1, 2, 3])
    l2 = deppy.add_const([1, 2, 3])
    add_node = deppy.add_node(add, loop_strategy=zip)

    deppy.add_edge(l1, add_node, "val1", loop=True)
    deppy.add_edge(l2, add_node, "val2", loop=True)

    result = await deppy.execute()
    assert result.query(add_node) == [2, 4, 6]


def test_constant_ignoreresult_no_children():
    def get_item():
        return IgnoreResult()

    def increment(data):
        return data + 1

    deppy = Deppy()

    item = deppy.add_node(get_item)
    increment_node = deppy.add_node(increment)

    deppy.add_edge(item, increment_node, "data")

    result = deppy.execute()
    assert result.query(increment_node) == []


async def test_constant_ignoreresult_no_children_async():
    def get_item():
        return IgnoreResult()

    async def increment(data):
        return data + 1

    deppy = Deppy()

    item = deppy.add_node(get_item)
    increment_node = deppy.add_node(increment)

    deppy.add_edge(item, increment_node, "data")

    result = await deppy.execute()
    assert result.query(increment_node) == []
