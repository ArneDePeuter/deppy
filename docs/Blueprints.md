# Blueprints

Blueprints provide a structured way to define a Deppy graph.
It creates a custom __init__ method depending on the defined objects, constants and secrets.
It creates a context manager based on the defined objects.
If there is an object with an async context manager, the blueprint will generate an async context manager instead.

Here is an example to illustrate the usage of blueprints:

```python
from deppy.blueprint import Blueprint, Node, Const, Secret, Output, Input, Object


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
    # Define objects which you can use to refer to the object's methods
    obj = Object(Obj)
    
    # Define constants and secrets
    const = Const()
    secret = Secret()
    
    items = Node(obj.get_list)
    item = Output(items, loop=True)
    
    add_node1 = Node(add)
    # Define inputs like this: via Input objects
    add_node2 = Node(add, inputs=[Input(add_node1, "a"), Input(item, "b")])

    # Define inputs like this: via edges
    edges = [
        (const, add_node1, "a"),
        (secret, add_node1, "b"),
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

The `Input` object has an optional field `input_name` which refers to the input parameter name of the function. By default it takes the name of the from_node.

For example:
```python
from deppy.blueprint import Blueprint, Node, Const, Secret, Input

def add(a, b):
    return a + b

class BP(Blueprint):
    a = Const()
    b = Secret()
    add_node = Node(add, inputs=[Input(a), Input(b)])

deppy = BP(a=10, b=20)
print(deppy.execute().query(deppy.add_node))  # [30]
```

Or even this is possible:
```python
from deppy.blueprint import Blueprint, Node, Const, Secret

def add(a, b):
    return a + b

class BP(Blueprint):
    a = Const()
    b = Secret()
    add_node = Node(add, inputs=[a, b])

deppy = BP(a=10, b=20)
print(deppy.execute().query(deppy.add_node))  # [30]
```

