from networkx import MultiDiGraph, is_directed_acyclic_graph
from typing import Optional, Callable, Union, Sequence, Any

from .node import Node
from .executor import Executor
from .scope import Scope


class Deppy:
    def __init__(self) -> None:
        self.graph = MultiDiGraph()
        self.executor = Executor(self.graph)
        self.const_counter = 0

    def node(self, func: Optional[Callable] = None, **kwargs) -> Union[Node, Callable[[Callable], Node]]:
        def decorator(f):
            node = Node(f, self, **kwargs)
            self.graph.add_node(node)
            assert is_directed_acyclic_graph(self.graph), "Circular dependency detected in the graph!"
            return node

        if func:
            return decorator(func)
        return decorator

    def edge(self, node1: Node, node2: Node, in_kwarg_name: str, loop: Optional[bool] = False, extractor: Optional[Callable[[Any], Any]] = None) -> None:
        if loop:
            node2.loop_vars.append((in_kwarg_name, node1))
        self.graph.add_edge(node1, node2, key=in_kwarg_name, loop=loop, extractor=extractor)
        assert is_directed_acyclic_graph(self.graph), "Circular dependency detected in the graph!"

    async def execute(self, *target_nodes: Sequence[Node]) -> Scope:
        return await self.executor.execute(*target_nodes)

    def dot(self, filename: str) -> None:
        from networkx.drawing.nx_pydot import write_dot
        dot_graph = self.graph.copy()
        for node in self.graph.nodes:
            for u, v, k, d in self.graph.edges(node, keys=True, data=True):
                if d["loop"]:
                    dot_graph.add_node(f"{u}->{v}", shape="circle", label=k)
                    dot_graph.add_edge(u, f"{u}->{v}", key=k)
                    dot_graph.add_edge(f"{u}->{v}", v, key=k)
                    dot_graph.remove_edge(u, v, k)
        write_dot(dot_graph, filename)

    def const(self, value: Any, secret: Optional[bool] = False, name: Optional[str] = None) -> Node:
        name = name or "CONST" + str(self.const_counter)
        node = self.node(lambda: value, name=name, secret=secret)
        self.const_counter += 1
        return node

    def secret(self, value: Any, name: Optional[str] = None) -> Node:
        return self.const(value, secret=True, name=name)
