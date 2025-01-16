from typing import Any, Callable, Optional, ParamSpec, TypeVar
from functools import wraps
from networkx import is_directed_acyclic_graph, MultiDiGraph
import inspect

from .node import Node


P = ParamSpec("P")
T = TypeVar("T")


class GraphBuilder:
    def __init__(self, graph: Optional[MultiDiGraph] = None) -> None:
        self.graph = graph or MultiDiGraph()
        self.consts_count = 0
        self.secrets_count = 0

        def add_wrapper(function: Callable[P, T]) -> Callable[P, Node]:
            @wraps(function)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> Node:
                node = function(*args, **kwargs)
                self.graph.add_node(node)
                return node

            return wrapper

        self.add_node = add_wrapper(Node)

    def check(self) -> None:
        if not is_directed_acyclic_graph(self.graph):
            raise ValueError("Circular dependency detected in the graph!")

    def add_output(
        self,
        node: Node,
        name: str,
        extractor: Optional[Callable[[Any], Any]] = lambda x: x,
        loop: Optional[bool] = False,
        secret: Optional[bool] = None,
    ) -> Node:
        node2 = self.add_node(func=extractor, name=name, secret=node.secret or secret)
        # get the parameter name of the extractor function
        parameters = inspect.signature(extractor).parameters
        if len(parameters) != 1:
            raise ValueError("Extractor function must have exactly one parameter")
        input_name = list(parameters.keys())[0]
        self.add_edge(node, node2, input_name, loop=loop)
        self.check()
        return node2

    def add_edge(
        self, node1: Node, node2: Node, input_name: str, loop: Optional[bool] = False
    ) -> None:
        if loop:
            node2.loop_vars.append((input_name, node1))
        self.graph.add_edge(node1, node2, key=input_name, loop=loop)
        self.check()

    def add_const(
        self, value: Optional[str] = None, name: Optional[Any] = None
    ) -> Node:
        name = name or "CONST" + str(self.consts_count)
        node = self.add_node(func=lambda: value, name=name, secret=False)
        self.consts_count += 1
        return node

    def add_secret(
        self, value: Optional[str] = None, name: Optional[Any] = None
    ) -> Node:
        name = name or "SECRET" + str(self.secrets_count)
        node = self.add_node(func=lambda: value, name=name, secret=True)
        self.secrets_count += 1
        return node
