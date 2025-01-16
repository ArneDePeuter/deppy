from deppy.deppy import Deppy
from deppy.executor import SyncExecutor


def test_execute_sync():
    deppy = Deppy()
    executor = SyncExecutor(deppy)

    # Create nodes in the graph
    node1 = deppy.add_const(value=5, name="ConstNode")
    node2 = deppy.add_node(func=lambda x: x * 2, name="MultiplyNode")
    node3 = deppy.add_node(func=lambda x: x + 1, name="IncrementNode")

    # Add edges to connect the nodes
    deppy.add_edge(node1, node2, input_name="x")
    deppy.add_edge(node2, node3, input_name="x")

    # Execute the graph synchronously
    root_scope = executor.execute_sync(node3)

    assert root_scope[node3] == 11  # (5 * 2) + 1


def test_execute_sync_threaded():
    deppy = Deppy()
    executor = SyncExecutor(deppy)

    # Create nodes in the graph
    node1 = deppy.add_const(value=5, name="ConstNode")
    node2 = deppy.add_node(func=lambda x: x * 2, name="MultiplyNode", to_thread=True)
    node3 = deppy.add_node(func=lambda x: x + 1, name="IncrementNode", to_thread=True)

    # Add edges to connect the nodes
    deppy.add_edge(node1, node2, input_name="x")
    deppy.add_edge(node2, node3, input_name="x")

    # Execute the graph synchronously
    root_scope = executor.execute_sync(node3)

    assert root_scope[node3] == 11  # (5 * 2) + 1

    executor.shutdown()
