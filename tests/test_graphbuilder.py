import pytest
from networkx import has_path
from deppy.graph_builder import GraphBuilder


def test_add_const():
    builder = GraphBuilder()
    const_node = builder.add_const(value="test_value", name="TestConst")

    assert const_node.name == "TestConst"
    assert const_node.func() == "test_value"
    assert const_node.secret is False
    assert builder.consts_count == 1

    new_const_node = builder.add_const(value="new_value")
    assert new_const_node.name == "CONST1"
    assert new_const_node.func() == "new_value"
    assert new_const_node.secret is False
    assert builder.consts_count == 2


def test_add_secret():
    builder = GraphBuilder()
    secret_node = builder.add_secret(value="secret_value", name="TestSecret")

    assert secret_node.name == "TestSecret"
    assert secret_node.func() == "secret_value"
    assert secret_node.secret is True
    assert builder.secrets_count == 1

    new_secret_node = builder.add_secret(value="new_secret")
    assert new_secret_node.name == "SECRET1"
    assert new_secret_node.func() == "new_secret"
    assert new_secret_node.secret is True
    assert builder.secrets_count == 2


def test_add_edge():
    builder = GraphBuilder()

    def func1():
        return 10

    def func2(input_value):
        return input_value * 2

    node1 = builder.add_const(value=5, name="ConstNode")
    node2 = builder.add_node(func=func2, name="Multiplier")

    builder.add_edge(node1, node2, input_name="input_value")
    edges = list(builder.graph.edges(data=True, keys=True))

    assert len(edges) == 1
    edge = edges[0]
    u, v, k, d = edge
    assert u == node1
    assert v == node2
    assert k == "input_value"
    assert d["loop"] is False


def test_add_output():
    builder = GraphBuilder()

    def extractor(input_value):
        return input_value + 1

    node1 = builder.add_const(value=10, name="InputNode")
    output_node = builder.add_output(node=node1, name="OutputNode", extractor=extractor)

    assert output_node.name == "OutputNode"
    assert has_path(builder.graph, node1, output_node) is True

    def invalid_extractor(input_value, extra_param):
        return input_value + 1

    with pytest.raises(
        ValueError, match="Extractor function must have exactly one parameter"
    ):
        builder.add_output(node=node1, name="OutputNode", extractor=invalid_extractor)


def test_check_circular_dependency():
    builder = GraphBuilder()

    def func1():
        return 10

    def func2(input_value):
        return input_value * 2

    node1 = builder.add_const(value=5, name="ConstNode")
    node2 = builder.add_node(func=func2, name="Multiplier")
    builder.add_edge(node1, node2, input_name="input_value")

    # Adding an edge back to create a cycle
    with pytest.raises(ValueError, match="Circular dependency detected in the graph!"):
        builder.add_edge(node2, node1, input_name="reverse")
