import pytest
from deppy.blueprint import Blueprint, Node, Const, Secret, Output, Input, Object


def add(a, b):
    return a + b


class Obj:
    def __init__(self, amount):
        self.list = list(range(amount))

    def get_list(self):
        return self.list


class BlueprintTest(Blueprint):
    obj = Object(Obj)

    const = Const()
    secret = Secret()

    add_node1 = Node(add)
    add_node2 = Node(add)
    items = Node(obj.get_list)
    item = Output(items, loop=True)

    edges = [
        (const, add_node1, "a"),
        (secret, add_node1, "b"),
        (add_node1, add_node2, "a"),
        (item, add_node2, "b"),
    ]


async def test_blueprint_execution():
    deppy = BlueprintTest(obj=Obj(3), const=3, secret=4)

    result = deppy.execute()

    # Verify outputs at each stage
    assert result.query(deppy.add_node1) == [7]
    assert result.query(deppy.items) == [[0, 1, 2]]
    assert result.query(deppy.add_node2) == [7, 8, 9]


def test_blueprint_initialization():
    deppy = BlueprintTest(obj=Obj(5), const=10, secret=20)

    assert isinstance(deppy.obj, Obj)
    assert deppy.obj.list == [0, 1, 2, 3, 4]
    assert deppy.const.name == "const"
    assert deppy.secret.name == "secret"


def test_blueprint_invalid_object_input():
    with pytest.raises(TypeError, match="got an unexpected keyword argument 'invalid'"):
        BlueprintTest(obj={"invalid": "input"})


def test_blueprint_resolve_node():
    deppy = BlueprintTest(obj=Obj(3), const=3, secret=4)

    resolved_node = deppy.resolve_node(BlueprintTest.add_node1)
    assert resolved_node is deppy.add_node1

    with pytest.raises(ValueError, match="Node 'add' not found in blueprint"):
        deppy.resolve_node(Node(add))


async def test_blueprint_context_management():
    class ObjWithContext:
        def __init__(self, amount):
            self.amount = amount
            self.entered = False
            self.exited = False

        def __enter__(self):
            self.entered = True
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.exited = True

        def get_amount(self):
            return self.amount

    class ContextBlueprint(Blueprint):
        obj = Object(ObjWithContext)

        amount_node = Node(obj.get_amount)
        edges = []

    with ContextBlueprint(obj=ObjWithContext(42)) as deppy:
        assert deppy.obj.entered

    assert deppy.obj.exited


async def test_blueprint_async_context_management():
    class ObjWithAsyncContext:
        def __init__(self, amount):
            self.amount = amount
            self.entered = False
            self.exited = False

        async def __aenter__(self):
            self.entered = True
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            self.exited = True

        def get_amount(self):
            return self.amount

    class ObjWithSyncContext:
        def __init__(self, amount):
            self.amount = amount
            self.entered = False
            self.exited = False

        def __enter__(self):
            self.entered = True
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.exited = True

        def get_amount(self):
            return self.amount

    class AsyncContextBlueprint(Blueprint):
        obj = Object(ObjWithAsyncContext)
        obj2 = Object(ObjWithSyncContext)

        amount_node = Node(obj.get_amount)
        edges = []

    async with AsyncContextBlueprint(
        obj=ObjWithAsyncContext(42), obj2=ObjWithSyncContext(42)
    ) as deppy:
        assert deppy.obj.entered
        assert deppy.obj2.entered

    assert deppy.obj.exited
    assert deppy.obj2.exited


def test_blueprint_edges_validation():
    class InvalidBlueprint(Blueprint):
        const = Const()
        add_node = Node(add)
        edges = [(const, add_node)]  # Missing the key argument for the edge

    with pytest.raises(
        AssertionError, match="Edges must be tuples with at least 3 elements"
    ):
        InvalidBlueprint(const=1)


def test_blueprint_outputs():
    deppy = BlueprintTest(obj=Obj(2), const=1, secret=2)

    result = deppy.execute()
    assert result.query(deppy.item) == [0, 1]


def test_invalid_object_passed_to_constructor():
    with pytest.raises(ValueError, match="Invalid input for object 'obj'"):
        BlueprintTest(obj=1)


def test_blueprint_input():
    def add(a, b):
        return a + b

    class BP(Blueprint):
        const = Const()
        b = Secret()
        add_node = Node(add, inputs=[Input(const, "a"), Input(b)])

    bp = BP(const=1, b=2)
    result = bp.execute()
    assert result.query(bp.add_node) == [3]
    assert result.query(bp.const) == [1]
    assert result.query(bp.b) == [2]


def test_blueprint_input_2():
    def add(a, b):
        return a + b

    class BP(Blueprint):
        a = Const()
        b = Secret()
        add_node = Node(add, inputs=[a, b])

    bp = BP(a=1, b=2)
    result = bp.execute()
    assert result.query(bp.add_node) == [3]


def test_blueprint_invalid_input():
    def add(a, b):
        return a + b

    class BP(Blueprint):
        a = Const()
        b = Secret()
        add_node = Node(add, inputs=[1, b])

    with pytest.raises(
        ValueError,
        match="Invalid input 1 for node 'add_node'. Must be Input or BlueprintObject",
    ):
        BP(a=1, b=2)
