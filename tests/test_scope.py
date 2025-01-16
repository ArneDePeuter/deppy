import pytest
from deppy.scope import Scope
from deppy.ignore_result import IgnoreResult
from deppy.node import Node


def test_query():
    scope = Scope()
    scope["key"] = "value"
    child = scope.birth()
    child["key"] = IgnoreResult("ignored")

    # Query without ignored results
    results = scope.query("key", ignored_results=False)
    assert results == ["value"]

    # Query with ignored results
    results = scope.query("key", ignored_results=True)
    assert len(results) == 1
    assert isinstance(results[0], IgnoreResult)

    # Query without specifying ignored_results
    results = scope.query("key")
    assert len(results) == 2
    assert "value" in results
    assert any(isinstance(result, IgnoreResult) for result in results)


def test_getitem():
    scope = Scope()
    scope["key"] = "value"

    assert scope["key"] == "value"

    # Test key in parent
    parent = Scope()
    parent["key_parent"] = "value_parent"
    child = Scope(parent=parent)
    assert child["key_parent"] == "value_parent"

    # Test missing key
    with pytest.raises(KeyError):
        _ = scope["missing"]


def test_dump():
    scope = Scope()
    scope["key"] = "value"

    node = Node(lambda: None)
    node.secret = True
    scope[node] = "sensitive_value"

    child = scope.birth()
    child["child_key"] = "child_value"

    # Without ignoring secrets
    dumped = scope.dump(ignore_secret=False)
    assert dumped["key"] == "value"
    assert dumped["children"][0]["child_key"] == "child_value"
    assert dumped[str(node)] == "***"

    # Ignoring secrets
    dumped = scope.dump(ignore_secret=True)
    assert dumped[str(node)] == "sensitive_value"


def test_birth():
    scope = Scope()
    child = scope.birth()

    assert child in scope.children
    assert child.parent is scope
    assert child.path == "$" + "/0"


def test_common_branch():
    parent = Scope()
    child1 = parent.birth()
    child2 = parent.birth()

    assert child1.common_branch(child2) is False
    assert child2.common_branch(child1) is False
    assert parent.common_branch(child1) is True
    assert parent.common_branch(child2) is True
