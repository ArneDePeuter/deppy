# Getting Started

Welcome to the **Deppy** library documentation!

## Installation

To use this library, ensure you have [uv](https://docs.astral.sh/uv/) installed.

Install Dependencies with:

```bash
uv sync
```

## Quick Start

Set up a new Deppy project:

```python
from deppy import Deppy

deppy = Deppy()


def get_list():
    return ["world!", "everyone!", "deppy!"]


def say_hello(to):
    return f"Hello {to}"


get_list_node = deppy.add_node(func=get_list)
say_hello_node = deppy.add_node(func=say_hello)
deppy.add_edge(get_list_node, say_hello_node, input_name="to", loop=True)

result = deppy.execute()
print(result.query(say_hello_node))  # ['Hello world!', 'Hello everyone!', 'Hello deppy!']
```
