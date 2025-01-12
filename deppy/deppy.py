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
