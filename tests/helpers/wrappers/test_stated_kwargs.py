import pytest
import os
from deppy.helpers.wrappers.stated_kwargs import StatedKwargs


@pytest.fixture
def state_file_path():
    return "test_state.json"


@pytest.fixture
def cleanup(state_file_path):
    yield
    if os.path.exists(state_file_path):
        os.remove(state_file_path)


@pytest.fixture
def stated_kwargs_instance(state_file_path, cleanup):
    return StatedKwargs(state_file=state_file_path)


def test_stated_kwarg(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(name="x", produce_function=produce_function)
    def test_function(x):
        return x

    with stated_kwargs_instance:
        result = test_function()
        assert result == 1

        result = test_function()
        assert result == 2

    with stated_kwargs_instance:
        result = test_function()
        assert result == 3


def test_stated_kwarg_with_initial_value(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value="init", produce_function=produce_function
    )
    def test_function(x):
        return x

    with stated_kwargs_instance:
        result = test_function()
        assert result == "init"

        result = test_function()
        assert result == 1

    with stated_kwargs_instance:
        result = test_function()
        assert result == 2


def test_stated_kwarg_with_keys(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value="init", produce_function=produce_function, keys=["a"]
    )
    def test_function(x, a):
        return x

    with stated_kwargs_instance:
        result = test_function(a=1)
        assert result == "init"

        result = test_function(a=2)
        assert result == "init"

        result = test_function(a=1)
        assert result == 1

        result = test_function(a=2)
        assert result == 2


def test_stated_kwarg_with_keys_composite(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x",
        initial_value="init",
        produce_function=produce_function,
        keys=["a", "b"],
    )
    def test_function(x, a, b):
        return x

    with stated_kwargs_instance:
        result = test_function(a=1, b=2)
        assert result == "init"

        result = test_function(a=1, b=3)
        assert result == "init"

        result = test_function(a=2, b=2)
        assert result == "init"

        result = test_function(a=1, b=2)
        assert result == 1

        result = test_function(a=1, b=3)
        assert result == 2

        result = test_function(a=2, b=2)
        assert result == 3


def test_stated_kwarg_from_result(stated_kwargs_instance):
    def produce_function(x):
        return x + 1

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value=0, produce_function=produce_function, from_result=True
    )
    def test_function(x):
        return x

    with stated_kwargs_instance:
        result = test_function()
        assert result == 0

        result = test_function()
        assert result == 1


def test_stated_kwarg_from_prev_state(stated_kwargs_instance):
    def produce_function(x):
        return x + 1

    @stated_kwargs_instance.stated_kwarg(
        name="x",
        initial_value=0,
        produce_function=produce_function,
        from_prev_state=True,
    )
    def test_function(x):
        return None

    with stated_kwargs_instance:
        test_function()
        current_value = stated_kwargs_instance._get(test_function, "x")
        assert current_value == 1

        test_function()
        current_value = stated_kwargs_instance._get(test_function, "x")
        assert current_value == 2


async def test_async_stated_kwarg(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(name="x", produce_function=produce_function)
    async def test_function(x):
        return x

    with stated_kwargs_instance:
        result = await test_function()
        assert result == 1

        result = await test_function()
        assert result == 2

    with stated_kwargs_instance:
        result = await test_function()
        assert result == 3


async def test_async_stated_kwarg_with_initial_value(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value="init", produce_function=produce_function
    )
    async def test_function(x):
        return x

    with stated_kwargs_instance:
        result = await test_function()
        assert result == "init"

        result = await test_function()
        assert result == 1

    with stated_kwargs_instance:
        result = await test_function()
        assert result == 2


async def test_async_stated_kwarg_with_keys(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value="init", produce_function=produce_function, keys=["a"]
    )
    async def test_function(x, a):
        return x

    with stated_kwargs_instance:
        result = await test_function(a=1)
        assert result == "init"

        result = await test_function(a=2)
        assert result == "init"

        result = await test_function(a=1)
        assert result == 1

        result = await test_function(a=2)
        assert result == 2


async def test_async_stated_kwarg_with_keys_composite(stated_kwargs_instance):
    counter = 0

    def produce_function():
        nonlocal counter
        counter += 1
        return counter

    @stated_kwargs_instance.stated_kwarg(
        name="x",
        initial_value="init",
        produce_function=produce_function,
        keys=["a", "b"],
    )
    async def test_function(x, a, b):
        return x

    with stated_kwargs_instance:
        result = await test_function(a=1, b=2)
        assert result == "init"

        result = await test_function(a=1, b=3)
        assert result == "init"

        result = await test_function(a=2, b=2)
        assert result == "init"

        result = await test_function(a=1, b=2)
        assert result == 1

        result = await test_function(a=1, b=3)
        assert result == 2

        result = await test_function(a=2, b=2)
        assert result == 3


async def test_async_stated_kwarg_from_result(stated_kwargs_instance):
    def produce_function(x):
        return x + 1

    @stated_kwargs_instance.stated_kwarg(
        name="x", initial_value=0, produce_function=produce_function, from_result=True
    )
    async def test_function(x):
        return x

    with stated_kwargs_instance:
        result = await test_function()
        assert result == 0

        result = await test_function()
        assert result == 1


async def test_async_stated_kwarg_from_prev_state(stated_kwargs_instance):
    def produce_function(x):
        return x + 1

    @stated_kwargs_instance.stated_kwarg(
        name="x",
        initial_value=0,
        produce_function=produce_function,
        from_prev_state=True,
    )
    async def test_function(x):
        return None

    with stated_kwargs_instance:
        await test_function()
        current_value = stated_kwargs_instance._get(test_function, "x")
        assert current_value == 1

        await test_function()
        current_value = stated_kwargs_instance._get(test_function, "x")
        assert current_value == 2
