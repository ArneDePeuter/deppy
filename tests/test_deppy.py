from deppy.deppy import Deppy


def test_deppy_resolve_dependency():
    deppy = Deppy()

    @deppy.node
    def test_node():
        return "resolved"

    assert deppy.resolve_dependency(test_node) == "resolved"


def test_deppy_call_with_dependencies():
    deppy = Deppy()

    @deppy.node
    def dependency():
        return "dependency_value"

    @deppy.node
    def test_node(dep=dependency):
        return f"result: {dep}"

    assert deppy.call_with_dependencies(test_node.func) == "result: dependency_value"


def test_deppy_register_node():
    deppy = Deppy()

    @deppy.node
    def test_node():
        return "node_registered"

    assert "test_node" in deppy.functions
    assert test_node.execute() == "node_registered"
