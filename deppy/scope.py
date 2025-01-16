from typing import Optional, Dict, Any, List

from .ignore_result import IgnoreResult
from .node import Node

import json


class Scope(dict):
    not_found = object()

    def __init__(self, parent: Optional[dict] = None, path: str = "$") -> None:
        self.parent = parent
        self.children: list["Scope"] = []
        self.path = path
        super().__init__()

    def query(self, key, ignored_results: Optional[bool] = None) -> List[Any]:
        values = []
        val = self.get(key, self.not_found)
        if val is not self.not_found and (
            ignored_results is None
            or (ignored_results and isinstance(val, IgnoreResult))
            or (not ignored_results and not isinstance(val, IgnoreResult))
        ):
            values.append(val)

        for child in self.children:
            values.extend(child.query(key, ignored_results=ignored_results))
        return values

    def __getitem__(self, item) -> Any:
        val = super().get(item, self.not_found)
        if val is not self.not_found:
            return val
        if self.parent is not None:
            return self.parent[item]
        raise KeyError(item)

    def dump(self, ignore_secret: Optional[bool] = False) -> Dict[str, Any]:
        return {
            str(key): "***"
            if isinstance(key, Node) and key.secret and not ignore_secret
            else value
            for key, value in self.items()
        } | (
            {"children": [child.dump(ignore_secret) for child in self.children]}
            if self.children
            else {}
        )

    def __str__(self) -> str:  # pragma: no cover
        return json.dumps(self.dump(), indent=2)

    def birth(self) -> "Scope":
        child = Scope(self, path=f"{self.path}/{len(self.children)}")
        self.children.append(child)
        return child

    def common_branch(self, other: "Scope") -> bool:
        if self is other:
            return True
        if self.path.startswith(other.path) or other.path.startswith(self.path):
            return True
        return False

    def __hash__(self) -> int:
        return id(self)

    def dot(
        self,
        filename: str,
        ignore_secret: Optional[bool] = False,
        max_label_size: int = 10,
    ) -> None:  # pragma: no cover
        import pydot

        graph = pydot.Dot(graph_type="digraph")

        def add_node(scope):
            data = scope.dump(ignore_secret)
            truncated = {
                k: (str(v)[:max_label_size] + "...")
                if len(str(v)) > max_label_size
                else v
                for k, v in data.items()
            }
            label = json.dumps(truncated, indent=2).replace('"', "").replace("'", "")
            node = pydot.Node(id(scope), label=label)
            graph.add_node(node)
            for child in scope.children:
                graph.add_edge(pydot.Edge(node, add_node(child)))
            return node

        add_node(self)
        graph.write(filename)
