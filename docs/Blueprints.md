# Blueprints

Blueprints provide a structured way to define a Deppy graph.
It creates a custom __init__ method depending on the defined objects, constants and secrets.
It creates a context manager based on the defined objects.

Here is an example to illustrate the usage of blueprints:

```python
from deppy.blueprint import Blueprint, Node, Const, Secret, Output, Object


def add(a, b):
    return a + b


class Obj:
    def __init__(self, amount):
        self.list = list(range(amount))

    def get_list(self):
        return self.list

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ExampleBlueprint(Blueprint):
    obj = Object(Obj)
    const = Const()
    secret = Secret()
    add_node1 = Node(add)
    add_node2 = Node(add)
    items = Node(obj.get_list)
    item = Output(items, loop=True)

    # Define edges (dependencies between nodes)
    edges = [
        (const, add_node1, "a"),
        (secret, add_node1, "b"),
        (add_node1, add_node2, "a"),
        (item, add_node2, "b"),
    ]

# Call generated __init__ method
# optionally you can also call like this: 
# deppy = ExampleBlueprint(obj={"amount": 5}, const=10, secret=20)

deppy = ExampleBlueprint(obj=Obj(5), const=10, secret=20)
# use generated context manager
with deppy:
    result = deppy.execute()
    print(result.query(deppy.add_node2))   # [30, 31, 32, 33, 34]
```

If there is an object with an async context manager, the blueprint will generate an async context manager instead.
