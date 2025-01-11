from typing import Optional, Dict, Any, List

from .ignore_result import IgnoreResult
from .node import Node


class Scope(dict):
    not_found = object()

    def __init__(self, parent: Optional[dict] = None) -> None:
        self.parent = parent
        self.children = []
        super().__init__()

    def __call__(self, key, ignored_results: Optional[bool] = None) -> List[Any]:
        values = []
        val = self.get(key)
        if val:
            if ignored_results is None:
                values.append(val)
            elif ignored_results and isinstance(val, IgnoreResult):
                values.append(val)
            elif not ignored_results and not isinstance(val, IgnoreResult):
                values.append(val)
        for child in self.children:
            values.extend(child(key, ignored_results=ignored_results))
        return values

    def __getitem__(self, item) -> Any:
        val = self.get(item, self.not_found)
        if val is not self.not_found:
            return val
        if self.parent is not None:
            return self.parent[item]
        raise KeyError(item)

    def dump(self, ignore_secret: Optional[bool] = False) -> Dict[str, Any]:
        cp: Dict[str, Any] = {}
        for key, value in self.items():
            if isinstance(key, Node) and key.secret and not ignore_secret:
                value = "***"
            cp[str(key)] = value
        if len(self.children) > 0:
            cp["children"] = [child.dump(ignore_secret) for child in self.children]
        return cp

    def __str__(self) -> str:
        return str(self.dump())

    def birth(self) -> 'Scope':
        child = Scope(self)
        self.children.append(child)
        return child

    def __hash__(self) -> int:
        return id(self)

    def dot(self, filename: str, ignore_secret: Optional[bool] = False) -> None:
        import pydot

        graph = pydot.Dot(graph_type='digraph')

        def add_node(scope):
            label = ""
            for key, value in scope.items():
                if isinstance(key, Node) and key.secret and not ignore_secret:
                    value = "***"
                label += f"{key}: {value}\n"
            node = pydot.Node(id(scope), label=label)
            graph.add_node(node)
            for child in scope.children:
                child_node = add_node(child)
                graph.add_edge(pydot.Edge(node, child_node))
            return node

        add_node(self)
        graph.write(filename)
