from deppy.helpers.fastapi import create_app
from deppy.blueprint import Blueprint, Object, Node, Output, Const, Secret


def add(a, b):
    return a + b


class Obj:
    def __init__(self, amount: int):
        self.list = list(range(amount))

    def get_list(self):
        return self.list


class BlueprintTest(Blueprint):
    obj = Object(Obj)

    const: int = Const()
    secret: int = Secret()

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


app = create_app(BlueprintTest)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
