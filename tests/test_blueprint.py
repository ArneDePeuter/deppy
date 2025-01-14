from deppy.blueprint import Blueprint, Node, Const, Secret, Output, Object


async def test_blueprint():
    class Obj:
        def __init__(self, amount):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

    def add(a, b):
        return a + b

    class TestBlueprint(Blueprint):
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

    deppy = TestBlueprint(obj=Obj(3), const=3, secret=4)

    assert not hasattr(deppy, "__aenter__")
    assert not hasattr(deppy, "__aexit__")
    assert not hasattr(deppy, "__enter__")
    assert not hasattr(deppy, "__exit__")

    result = deppy.execute()

    assert result.query(deppy.add_node1) == [7]
    assert result.query(deppy.items) == [[0, 1, 2]]
    assert result.query(deppy.add_node2) == [7, 8, 9]

