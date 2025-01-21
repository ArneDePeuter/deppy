from typing import Any, Callable, Optional
from networkx import is_directed_acyclic_graph, MultiDiGraph
import inspect

from .node import Node, LoopStrategy, product


class GraphBuilder:
    """
    A class to build and validate directed acyclic graphs (DAGs) using computational nodes.

    Attributes
    ----------
    graph : MultiDiGraph
        The underlying graph structure.
    consts_count : int
        Counter for the number of constant nodes added.
    secrets_count : int
        Counter for the number of secret nodes added.
    """

    def __init__(self, graph: Optional[MultiDiGraph] = None) -> None:
        """
        Constructs a new GraphBuilder instance.

        Parameters
        ----------
        graph : Optional[MultiDiGraph], optional
            An existing MultiDiGraph instance to use as the base graph (default is None).
        """
        self.graph = graph or MultiDiGraph()
        self.consts_count = 0
        self.secrets_count = 0

    def add_node(
        self,
        func: Callable[..., Any],
        loop_strategy: Optional[LoopStrategy] = product,
        to_thread: Optional[bool] = False,
        name: Optional[str] = None,
        secret: Optional[bool] = False,
    ) -> Node:
        """
        Constructs a new Node object and adds it to the graph and returns it.

        Parameters
        ----------
        func : Callable[..., Any]
            The function executed by the node.
        loop_strategy : Optional[LoopStrategy], optional
            The strategy used for joining looped variables (default is cartesian product).
        to_thread : Optional[bool], optional
            Flag to execute the function in a separate thread (default is False). No effect if the function is asynchronous.
        name : Optional[str], optional
            The name of the node (default is the function's name).
        secret : Optional[bool], optional
            Indicates whether the node is sensitive and should be masked in outputs (default is False).
        """
        node = Node(
            func=func,
            loop_strategy=loop_strategy,
            to_thread=to_thread,
            name=name,
            secret=secret,
        )
        self.graph.add_node(node)
        return node

    def check(self) -> None:
        """
        Validates the graph to ensure it is a directed acyclic graph (DAG).

        Raises
        ------
        ValueError
            If a circular dependency is detected in the graph.
        """
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
        """
        Adds an output node to the graph with a specified extractor function.

        Parameters
        ----------
        node : Node
            The input node to extract data from.
        name : str
            The name of the output node.
        extractor : Optional[Callable[[Any], Any]], optional
            A function to extract or transform data (default is identity function).
        loop : Optional[bool], optional
            Whether the edge represents a loop variable (default is False).
        secret : Optional[bool], optional
            Whether the output node is marked as secret (default is None).

        Returns
        -------
        Node
            The newly created output node.

        Raises
        ------
        ValueError
            If the extractor function does not have exactly one parameter.
        """
        node2 = self.add_node(func=extractor, name=name, secret=node.secret or secret)
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
        """
        Adds a directed edge between two nodes in the graph.

        Parameters
        ----------
        node1 : Node
            The source node.
        node2 : Node
            The target node.
        input_name : str
            The input name for the edge.
        loop : Optional[bool], optional
            Whether the edge represents a loop variable (default is False).
        """
        if loop:
            node2.loop_vars.append((input_name, node1))
        self.graph.add_edge(node1, node2, key=input_name, loop=loop)
        self.check()

    def add_const(
        self, value: Optional[str] = None, name: Optional[Any] = None
    ) -> Node:
        """
        Adds a constant node to the graph.

        Parameters
        ----------
        value : Optional[str], optional
            The constant value (default is None).
        name : Optional[Any], optional
            The name of the constant node (default is auto-generated).

        Returns
        -------
        Node
            The newly created constant node.
        """
        name = name or "CONST" + str(self.consts_count)
        node = self.add_node(func=lambda: value, name=name, secret=False)
        self.consts_count += 1
        return node

    def add_secret(
        self, value: Optional[str] = None, name: Optional[Any] = None
    ) -> Node:
        """
        Adds a secret node to the graph.

        Parameters
        ----------
        value : Optional[str], optional
            The secret value (default is None).
        name : Optional[Any], optional
            The name of the secret node (default is auto-generated).

        Returns
        -------
        Node
            The newly created secret node.
        """
        name = name or "SECRET" + str(self.secrets_count)
        node = self.add_node(func=lambda: value, name=name, secret=True)
        self.secrets_count += 1
        return node
