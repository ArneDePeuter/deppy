from typing import Optional
import asyncio

from .node import Node
from .graph_builder import GraphBuilder
from .executor import HybridExecutor


class Deppy:
    """
    A class to manage the creation and execution of dependency graphs.

    Attributes
    ----------
    _name : Optional[str]
        The name of the Deppy instance.
    graph_builder : GraphBuilder
        Instance of GraphBuilder for managing the dependency graph.
    graph : MultiDiGraph
        The underlying graph structure managed by the GraphBuilder.
    executor : HybridExecutor
        Instance of HybridExecutor for executing the dependency graph.
    """

    def __init__(self, name: Optional[str] = "Deppy") -> None:
        """
        Constructs a Deppy instance with a dependency graph and executor.

        Parameters
        ----------
        name : Optional[str], optional
            The name of the Deppy instance (default is "Deppy").
        """
        self._name = name

        self.graph_builder = GraphBuilder()
        self.graph = self.graph_builder.graph
        self.add_node = self.graph_builder.add_node
        self.add_output = self.graph_builder.add_output
        self.add_edge = self.graph_builder.add_edge
        self.add_const = self.graph_builder.add_const
        self.add_secret = self.graph_builder.add_secret

        self.executor = HybridExecutor(self)

    def get_node_by_name(self, name: str) -> Optional[Node]:
        """
        Retrieves a node from the graph by its name.

        Parameters
        ----------
        name : str
            The name of the node to retrieve.

        Returns
        -------
        Optional[Node]
            The node with the specified name, or None if not found.
        """
        for node in self.graph.nodes:
            if node.name == name:
                return node
        return None

    def dot(self, filename: str) -> None:  # pragma: no cover
        """
        Exports the dependency graph to a DOT file.

        Parameters
        ----------
        filename : str
            The name of the file to write the DOT representation to.
        """
        from networkx.drawing.nx_pydot import write_dot

        dot_graph = self.graph.copy()
        for node in self.graph.nodes:
            for u, v, k, d in self.graph.edges(node, keys=True, data=True):
                if d["loop"]:
                    d = {
                        "color": "red",
                        "style": "bold",
                        "penwidth": 2,
                        "arrowhead": "diamond",
                    }
                    dot_graph.add_edge(u, v, key=k, **d)
        write_dot(dot_graph, filename)

    def execute_is_async(self) -> bool:
        """
        Checks whether the execute method is asynchronous.

        Returns
        -------
        bool
            True if the execute method is asynchronous, False otherwise.
        """
        return asyncio.iscoroutinefunction(self.execute)

    @property
    def execute(self):
        """
        Determines the appropriate execution method for the graph.

        Returns
        -------
        Callable
            The synchronous or asynchronous execution method, depending on the graph's nodes.
        """
        has_async_nodes = any(node.is_async for node in self.graph.nodes)
        if not has_async_nodes:
            return self.executor.execute_sync
        all_async_nodes = all(node.is_async for node in self.graph.nodes)
        if all_async_nodes:
            return self.executor.execute_async
        return self.executor.execute_hybrid
