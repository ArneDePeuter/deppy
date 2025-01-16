from deppy.deppy import Deppy
from deppy.executor import HybridExecutor


async def test_execute_hybrid():
    deppy = Deppy()
    executor = HybridExecutor(deppy)

    # Create nodes in the graph

    node1 = deppy.add_const(value=5, name="ConstNode")

    def multiply(x):
        return x * 2

    async def increment(x):
        return x + 1

    node2 = deppy.add_node(func=multiply, name="MultiplyNode")
    node3 = deppy.add_node(func=increment, name="IncrementNode")

    deppy.add_edge(node1, node2, input_name="x")
    deppy.add_edge(node2, node3, input_name="x")

    root_scope = await executor.execute_hybrid()

    assert root_scope[node3] == 11  # (5 * 2) + 1
