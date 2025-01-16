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

---

