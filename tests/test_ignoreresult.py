from deppy.ignore_result import IgnoreResult


def test_ignore_result_initialization():
    result = IgnoreResult(reason="TestReason", data={"key": "value"})

    assert result.reason == "TestReason"
    assert result.data == {"key": "value"}


def test_ignore_result_default_initialization():
    result = IgnoreResult()

    assert result.reason is None
    assert result.data is None


def test_ignore_result_str_representation():
    result = IgnoreResult(reason="TestReason", data={"key": "value"})
    assert str(result) == "IgnoreResult(TestReason, {'key': 'value'})"


def test_ignore_result_repr():
    result = IgnoreResult(reason="TestReason", data={"key": "value"})
    assert repr(result) == "IgnoreResult(TestReason, {'key': 'value'})"
