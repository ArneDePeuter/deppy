from typing import Optional

from .node import Node
from .multidag_builder import GraphBuilder
from .executor import Executor


class Deppy:
    def __init__(self) -> None:
        self.graph_builder = GraphBuilder()
        self.graph = self.graph_builder.graph
        self.add_node = self.graph_builder.add_node
        self.add_output = self.graph_builder.add_output
        self.add_edge = self.graph_builder.add_edge
        self.add_const = self.graph_builder.add_const
        self.add_secret = self.graph_builder.add_secret

        self.executor = Executor(self.graph)
        self.execute = self.executor.execute

    def get_node_by_name(self, name: str) -> Optional[Node]:
        for node in self.graph.nodes:
            if node.name == name:
                return node
        return None

    def dot(self, filename: str) -> None:  # pragma: no cover
        from networkx.drawing.nx_pydot import write_dot
        dot_graph = self.graph.copy()
        for node in self.graph.nodes:
            for u, v, k, d in self.graph.edges(node, keys=True, data=True):
                if d["loop"]:
                    d = {"color": "red", "style": "bold", "penwidth": 2, "arrowhead": "diamond"}
                    dot_graph.add_edge(u, v, key=k, **d)
        write_dot(dot_graph, filename)
