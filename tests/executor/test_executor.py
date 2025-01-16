from networkx import MultiDiGraph
from deppy.node import Node
from deppy.scope import Scope
from deppy.executor.executor import Executor
from deppy.ignore_result import IgnoreResult
from deppy import Deppy


def test_batched_topological_order():
    deppy = Deppy()
    node1 = deppy.add_node(func=lambda: 1, name="Node1")
    node2 = deppy.add_node(func=lambda: 2, name="Node2")
    node3 = deppy.add_node(func=lambda x: x, name="Node3")
    deppy.add_edge(node1, node3, "x")

    executor = Executor(deppy=deppy)
    executor.setup()

    batches = list(executor.batched_topological_order())

    assert len(batches) == 2
    assert batches[0] == {node1, node2}
    assert batches[1] == {node3}


def test_save_results_without_loop():
    scope = Scope()
    node = Node(func=lambda: 1, name="Node1")
    results = [42]

    saved_scopes = Executor.save_results(node, results, scope)

    assert scope[node] == 42
    assert len(saved_scopes) == 1
    assert scope in saved_scopes


def test_save_results_with_loop():
    scope = Scope()
    node = Node(func=lambda: 1, name="Node1")
    node.loop_vars = [("var", node)]
    results = [10, 20]

    saved_scopes = Executor.save_results(node, results, scope)

    assert len(saved_scopes) == 2
    for result, saved_scope in zip(results, saved_scopes):
        assert saved_scope[node] == result


def test_create_flow_graph():
    def mock_deppy():
        pass

    mock_deppy.graph = MultiDiGraph()
    executor = Executor(deppy=mock_deppy)

    node1 = Node(func=lambda: 1, name="Node1")
    node2 = Node(func=lambda: 2, name="Node2")
    mock_deppy.graph.add_edge(node1, node2)

    flow_graph = executor.create_flow_graph(node2)

    assert node1 in flow_graph
    assert node2 in flow_graph
    assert len(flow_graph.nodes) == 2
    assert len(flow_graph.edges) == 1


def test_setup():
    def mock_deppy():
        pass

    mock_deppy.graph = MultiDiGraph()
    executor = Executor(deppy=mock_deppy)

    node1 = Node(func=lambda: 1, name="Node1")
    node2 = Node(func=lambda: 2, name="Node2")
    mock_deppy.graph.add_edge(node1, node2)

    executor.setup(node2)

    assert executor.flow_graph.has_edge(node1, node2)
    assert isinstance(executor.root, Scope)


def test_resolve_args_without_loop():
    executor = Executor(deppy=None)
    graph = MultiDiGraph()
    node1 = Node(func=lambda: 1, name="Node1")
    node2 = Node(func=lambda x: x + 1, name="Node2")
    graph.add_edge(node1, node2, key="input_value")
    executor.flow_graph = graph

    scope = Scope()
    scope[node1] = 42

    resolved_args = executor.resolve_args(node2, scope)

    assert len(resolved_args) == 1
    assert resolved_args[0] == {"input_value": 42}


def test_resolve_args_with_loop():
    executor = Executor(deppy=None)
    graph = MultiDiGraph()
    node1 = Node(func=lambda: 1, name="Node1")
    node2 = Node(func=lambda x, y: x + y, name="Node2")
    graph.add_edge(node1, node2, key="x")
    executor.flow_graph = graph

    node2.loop_vars = [("x", node1)]
    node2.loop_strategy = lambda x: [(val,) for val in x]

    scope = Scope()
    scope[node1] = [1, 2, 3]

    resolved_args = executor.resolve_args(node2, scope)

    assert len(resolved_args) == 3
    assert resolved_args[0]["x"] == 1
    assert resolved_args[1]["x"] == 2
    assert resolved_args[2]["x"] == 3


def test_save_results_with_ignore_result():
    scope = Scope()
    node = Node(func=lambda: 1, name="Node1")
    ignore_result_instance = IgnoreResult()
    results = [42, ignore_result_instance]

    saved_scopes = Executor.save_results(node, results, scope)

    # Check that the valid result is saved in the scope
    assert scope[node] == 42

    # Check that the IgnoreResult instance is skipped
    assert len(saved_scopes) == 1
    saved_scope = next(iter(saved_scopes))
    assert node in saved_scope
    assert saved_scope[node] == 42

    # Ensure the IgnoreResult instance does not create a scope
    assert ignore_result_instance not in saved_scope.values()
