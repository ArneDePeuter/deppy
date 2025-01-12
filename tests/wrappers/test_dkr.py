from deppy.helpers.wrappers import StringDk, MappingDk, IterDk, JsonDk, Dkr


def test_string_dk_resolution():
    dk = StringDk("{greeting}")
    assert dk.resolve({"greeting": "Hello, Alice!"}) == "Hello, Alice!"

    dk_static = StringDk("Static value")
    assert dk_static.resolve({}) == "Static value"

    dk_full_emplace = StringDk("{val}")
    obj = object()
    assert dk_full_emplace.resolve({"val": obj}) is obj


def test_mapping_dk_resolution():
    data = {"key": "value", "nested_key": "nested_value"}
    nested_skd = StringDk("{nexted_key}")
    mapping = {
        "key": StringDk("{key}"),
        "nested": {"inner": nested_skd},
    }
    dk = MappingDk(mapping)
    resolved = dk.resolve(data)

    assert resolved == {
        "key": "value",
        "nested": {"inner": nested_skd},
    }


def test_iter_dk_resolution():
    data = {"item1": "val1", "item2": "val2"}
    dk = IterDk([StringDk("{item1}"), StringDk("{item2}"), "static"])
    resolved = dk.resolve(data)

    assert resolved == ["val1", "val2", "static"]


def test_json_dk_resolution():
    data = {"name": "Alice", "age": 30}
    json_data = {
        "person": {"name": "{name}", "age": "{age}"},
        "list": ["{name}", "{age}"]
    }
    dk = JsonDk(json_data)
    resolved = dk.resolve(data)

    assert resolved == {
        "person": {"name": "Alice", "age": 30},
        "list": ["Alice", 30]
    }


def test_dkr_resolution():
    data = {"x": 10, "y": 20}
    dkr = Dkr(x=StringDk("{x}"), y=StringDk("{y}"), z=30)
    resolved = dkr.resolve(data)

    assert resolved == {"x": 10, "y": 20, "z": 30}


def test_dkr_wraps_sync():
    data = {"name": "Alice"}

    def greet(name):
        return f"Hello, {name}!"

    dkr = Dkr(name=StringDk("{name}"))
    wrapped_func = dkr.wraps(greet)
    result = wrapped_func(**data)

    assert result == "Hello, Alice!"


async def test_dkr_wraps_async():
    data = {"name": "Alice"}

    async def greet(name):
        return f"Hello, {name}!"

    dkr = Dkr(name=StringDk("{name}"))
    wrapped_func = dkr.wraps(greet)
    result = await wrapped_func(**data)

    assert result == "Hello, Alice!"
