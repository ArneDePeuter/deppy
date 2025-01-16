# Deppy
[![Coverage](https://img.shields.io/badge/Coverage-100%25-brightgreen)]() [![Code Quality](https://img.shields.io/badge/Code%20Quality-A%2B-blue)]()

🚀 **Deppy** is a cutting-edge **dependency executor for Python** that empowers developers to design, build, and execute **DAGs (Directed Acyclic Graphs)** efficiently, effectively, and effortlessly.

Deppy is a versatile tool for designing complex workflows, including:
 - 📊 ETL Pipelines: Seamlessly extract, transform, and load data across systems.

### 🌟 Features:
- 🛠️ **Graph Building**: Create complex workflows with minimal effort.
- ⚡ **Optimized Execution**: Executes dependencies in the optimal order, leveraging threads and concurrency for maximum performance.
- 🖼️ **Graph Visualization**: Gain insights into your workflows with intuitive visual representations of your DAGs.
- 🔄 **Flexible Workflow Design**: Seamlessly supports synchronous, asynchronous, and hybrid workflows.
- 🎯 **Advanced Utilities**: Packed with tools to streamline and supercharge your development experience.
- 🛡️ **Reliable**: Comprehensive test coverage ensures stability and reliability.

### 📦 Installation

Deppy has support for python 3.11 and above.

To use this library, ensure you have [uv](https://docs.astral.sh/uv/) installed.

Install Dependencies with:

```bash
uv sync
```

### 🚀 Quick Start

A quick and easy example to get you started with Deppy:

```python
from deppy import Deppy

def get_list():
    return ["world!", "everyone!", "deppy!"]

def say_hello(to):
    return f"Hello {to}"

deppy = Deppy()

get_list_node = deppy.add_node(func=get_list)
say_hello_node = deppy.add_node(func=say_hello)
deppy.add_edge(get_list_node, say_hello_node, input_name="to", loop=True)

result = deppy.execute()
print(result.query(say_hello_node))  # ['Hello world!', 'Hello everyone!', 'Hello deppy!']
```

### 📖 Documentation
For detailed usage and advanced features, dive into our [comprehensive documentation](./docs).

### 📚 Examples
Explore our [examples](./examples) to see Deppy in action.

---

⚡ Transform your workflow management with **Deppy** today!
