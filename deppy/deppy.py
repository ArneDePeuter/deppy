from networkx import MultiDiGraph, is_directed_acyclic_graph
from itertools import product
from typing import Optional, Callable, Union, Sequence, Any

from .node import Node, LoopStrategy
from .executor import Executor
from .cache import Cache
from .scope import Scope


class Deppy:
    def __init__(self) -> None:
        self.graph = MultiDiGraph()
        self.executor = Executor(self.graph)

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
        write_dot(self.graph, filename)
