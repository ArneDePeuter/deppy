# DLT: blueprint to DLT source

The `blueprint_to_source` utility bridges the gap between `Blueprint` objects and `dlt` sources. 
It automatically generates a `dlt source` from a `Blueprint` instance.

It will automatically create a config spec based on the objects, secrets and consts defined in the blueprint.
It will handle context management properly and will follow optional execution settings.

```python
def blueprint_to_source(
    blueprint: Type[Blueprint],
    target_nodes: Optional[Iterable[Node]] = None,
    exclude_for_storing: Optional[Iterable[Node]] = None
) -> DltSource:
```

### **Parameters**
- **`blueprint`**: The `Blueprint` class to be converted into a `dlt` source.
- **`target_nodes`** (Optional): A list of nodes to be executed. If not provided, all nodes in the graph are executed.
- **`exclude_for_storing`** (Optional): A list of nodes to exclude from storage in the `dlt` source.

Secret nodes are automatically excluded from storing.

For proper configuration use type annotations so the types can be properly converted.
To type an object simply add type annotations to the object's __init__ parameters.
To type a config and a secret add the type annotation to the definition.

Example:
```python
from deppy.blueprint import Blueprint, Object, Node, Const, Secret, Output
from deppy.helpers.DLT import blueprint_to_source
import dlt

def add(a, b):
    return a + b

class Obj:
    def __init__(self, amount: int):
        self.list = list(range(amount))

    def get_list(self):
        return self.list

class Example(Blueprint):
    obj = Object(Obj)

    my_const: int = Const()
    my_secret: int = Secret()

    add_node1 = Node(add)
    add_node2 = Node(add)
    items = Node(obj.get_list)
    item = Output(items, loop=True)

    edges = [
        (my_const, add_node1, "a"),
        (my_secret, add_node1, "b"),
        (add_node1, add_node2, "a"),
        (item, add_node2, "b"),
    ]

source = blueprint_to_source(Example)

pipeline = dlt.pipeline(pipeline_name="my_example", destination="duckdb", full_refresh=True)
pipeline.run(source())
```
---


To pass extra kwargs for the resource for a specific node you can use the `extra_kwargs` parameter.

For example to define a primary key for a resource:
```python
blueprint_to_source(SomeBlueprint, resource_kwargs={SomeBlueprint.item: {"primary_key": "id"}})
```

For more extra kwargs check the `dlt` documentation.
