from deppy.deppy import Deppy
from deppy.executor import AsyncExecutor


async def test_execute_async():
    deppy = Deppy()
    executor = AsyncExecutor(deppy)

    # Create nodes in the graph

    async def get_value():
        return 5

    node1 = deppy.add_node(func=get_value, name="ConstNode")

    async def multiply(x):
        return x * 2

    async def increment(x):
        return x + 1

    node2 = deppy.add_node(func=multiply, name="MultiplyNode")
    node3 = deppy.add_node(func=increment, name="IncrementNode")

    # Add edges to connect the nodes
    deppy.add_edge(node1, node2, input_name="x")
    deppy.add_edge(node2, node3, input_name="x")

    # Execute the graph synchronously
    root_scope = await executor.execute_async()

    assert root_scope[node3] == 11  # (5 * 2) + 1


async def test_execute_async_with_max_concurrent_tasks():
    deppy = Deppy()
    executor = AsyncExecutor(deppy, max_concurrent_tasks=2)

    # Create nodes in the graph

    async def get_value():
        return 5

    node1 = deppy.add_node(func=get_value, name="ConstNode")

    async def multiply(x):
        return x * 2

    async def increment(x):
        return x + 1

    node2 = deppy.add_node(func=multiply, name="MultiplyNode")
    node3 = deppy.add_node(func=increment, name="IncrementNode")

    # Add edges to connect the nodes
    deppy.add_edge(node1, node2, input_name="x")
    deppy.add_edge(node2, node3, input_name="x")

    # Execute the graph synchronously
    root_scope = await executor.execute_async()

    assert root_scope[node3] == 11  # (5 * 2) + 1
