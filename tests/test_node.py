from deppy.deppy import Deppy


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
