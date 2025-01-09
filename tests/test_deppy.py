import asyncio
from deppy.deppy import Deppy


def test_deppy_register_node():
    deppy = Deppy()

    @deppy.node
    def test_node():
        return "node_registered"

    assert "test_node" in deppy.functions
    assert asyncio.run(deppy.execute()) == {"test_node": "node_registered"}


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
    assert result == {"node1": "node1_result", "node2": "node2_result: node1_result"}
