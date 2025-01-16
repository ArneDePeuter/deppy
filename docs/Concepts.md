# Concepts

Deppy has several core concepts:

### 1. Deppy
The main interface for managing and executing dependency graphs.

This is the place where you add nodes, consts, secrets, outputs and edges.

You can visualise the current deppy using the `deppy.dot` method. 
This will generate a graph in the DOT format, which can be rendered using Graphviz.
A really handy utility for debugging your workflow, or just to see how the graph looks like.

You can also execute the graph using the `deppy.execute` method.
Depending on the graph, this can be a synchronous or asynchronous operation.
Optionally you can pass the nodes it should only execute. Then deppy will execute only those nodes and their dependencies.
If no nodes are passed, it will execute all nodes in the deppy.

Edges which are looped are marked with a red color.

An example:
```python
from deppy import Deppy

def multiply(val1, val2):
    return val1 * val2

def increment(val):
    return val + 1

deppy = Deppy()

l1_node = deppy.add_const([1, 2, 3], name="l1")
l2_node = deppy.add_node([2, 3, 4], name="l2")
multiply_node = deppy.add_node(multiply)
increment_node = deppy.add_node(increment)

deppy.add_edge(l1_node, multiply_node, "val1", loop=True)
deppy.add_edge(l2_node, multiply_node, "val2", loop=True)
deppy.add_edge(multiply_node, increment_node, "val")

deppy.dot("test_output.dot")
```
This would output following graph:

<img src="img.png" width="200"/>

Deppy will execute the graph as efficiently as possible.
It will execute nodes in the correct order, based on their dependencies.
It will try to execute nodes concurrently, if possible. And execute nodes in a separate thread if asked to.

### 2. Node
The node holds the business logic of the graph.

When creating a node there are following parameters:
- `func`: The function to be executed.
- `loop_strategy`: The strategy to be used for joining looped inputs. Default is `cartesian`.
- `to_thread`: Whether to execute the function in a separate thread. Default is `False`. (Async functions are executed concurrently so this parameter is ignored for them)
- `name`: The name of the node. Default is the name of the function.
- `secret`: Whether the output of the node should be masked. Default is `False`.

But rather than creating a node directly, you can use the `deppy.add_node` method.

### 3. Edge
A connection between nodes, defining dependencies.
You can create an edge using the `deppy.add_edge` method.

This takes following parameters:
- `node1`: The source node.
- `node2`: The destination node.
- `input_name`: The kwarg name to which the output of the source node should be passed.
- `loop`: Whether there should be looped on the result from the source node. Default is `False`.

### 4. Output
A derived value extracted from a node.
You can create an output using the `deppy.add_output` method.

This takes following parameters:
- `node`: The node from which the output is derived.
- `name`: The name of the output. Default is the name of the extractor method.
- `extractor`: The method to extract the output from the node. If not passed it will simply forward the output of the node.
- `loop`: Whether there should be looped on the output. Default is `False`.
- `secret`: Whether the output should be masked. Default is `node.secret`.

### 3. Const
A constant is basically a node with a fixed output.
Rather than creating a node directly, you can use the `deppy.add_const` method.

This takes following parameters:
- `value`: The value of the constant.
- `name`: The name of the constant. Default is a generated name.

### 4. Secret
A secret is basically a node with a masked output.
Rather than creating a node directly, you can use the `deppy.add_secret` method.

This takes following parameters:
- `value`: The value of the secret.
- `name`: The name of the secret. Default is a generated name.

### 5. IgnoreData

A special class that can be used to ignore data.
If a node receives an `IgnoreData` object, it will no further execute this branch of the graph.
