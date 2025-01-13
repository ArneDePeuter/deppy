from typing import Any, Callable, Optional, ParamSpec, TypeVar, Dict
from functools import wraps
from networkx import is_directed_acyclic_graph, MultiDiGraph
import inspect

from .node import Node


P = ParamSpec("P")
T = TypeVar("T")


class GraphBuilder:
    def __init__(self, graph: Optional[MultiDiGraph] = None) -> None:
        self.graph = graph or MultiDiGraph()
        self.consts = {}
        self.secrets = {}

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

    def add_output(self, node: Node, name: str, extractor: Optional[Callable[[Any], Any]], loop: Optional[bool] = False, secret: Optional[bool] = None) -> Node:
        node2 = self.add_node(func=extractor, name=name, secret=node.secret or secret)
        # get the parameter name of the extractor function
        parameters = inspect.signature(extractor).parameters
        if len(parameters) != 1:
            raise ValueError("Extractor function must have exactly one parameter")
        input_name = list(parameters.keys())[0]
        self.add_edge(node, node2, input_name, loop=loop)
        self.check()
        return node2

    def add_edge(self, node1: Node, node2: Node, input_name: str, loop: Optional[bool] = False) -> None:
        if loop:
            node2.loop_vars.append((input_name, node1))
        self.graph.add_edge(node1, node2, key=input_name, loop=loop)
        self.check()

    def add_const(self, value: Optional[str] = None, name: Optional[Any] = None) -> Node:
        name = name or "CONST" + str(len(self.consts))
        node = self.add_node(func=lambda: value, name=name, secret=False)
        self.consts[name] = node
        return node

    def add_secret(self, value: Optional[str] = None, name: Optional[Any] = None) -> Node:
        name = name or "SECRET" + str(len(self.secrets))
        node = self.add_node(func=lambda: value, name=name, secret=True)
        self.secrets[name] = node
        return node

    def configure(self, **kwargs: Dict[str, Any]):
        def make_lambda(val):
            # separate function for lambda creation to avoid late binding
            return lambda: val

        for key, value in kwargs.items():
            if key in self.consts:
                self.consts[key].func = make_lambda(value)
            elif key in self.secrets:
                self.secrets[key].func = make_lambda(value)
            else:
                raise ValueError(f"Unknown configuration key {key}")
