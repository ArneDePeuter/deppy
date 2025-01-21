from typing import Optional, Dict, Any, List
import pydot
import json

from .ignore_result import IgnoreResult
from .node import Node


class Scope(dict):
    """
    A class representing a hierarchical scope structure.

    Attributes
    ----------
    not_found : object
        Sentinel object used to represent a missing key.
    parent : Optional[dict]
        Optional parent scope to inherit from.
    children : list[Scope]
        List of child scopes.
    path : str
        Path identifying the current scope.
    """

    not_found = object()

    def __init__(
        self, parent: Optional[dict] = None, path: Optional[str] = "$"
    ) -> None:
        """
        Constructs a new Scope object.

        Parameters
        ----------
        parent : Optional[dict], optional
            The parent scope to inherit from (default is None).
        path : Optional[str], optional
            Path identifying the current scope (default is "$" for root).
        """
        self.parent = parent
        self.path = path
        self.children: list["Scope"] = []
        super().__init__()

    def query(self, key, ignored_results: Optional[bool] = None) -> List[Any]:
        """
        Queries the scope and its children for a specified key.

        Parameters
        ----------
        key : any
            The key to search for.
        ignored_results : Optional[bool], optional
            Flag to filter results based on their type (default is None).

        Returns
        -------
        List[Any]
            A list of matching values from the current scope and its children.
        """
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
        """
        Retrieves the value associated with a key, searching parent scopes if necessary.

        Parameters
        ----------
        item : any
            The key to retrieve.

        Returns
        -------
        Any
            The value associated with the key.

        Raises
        ------
        KeyError
            If the key is not found in the current or parent scopes.
        """
        val = super().get(item, self.not_found)
        if val is not self.not_found:
            return val
        if self.parent is not None:
            return self.parent[item]
        raise KeyError(item)

    def dump(self, mask_secrets: Optional[bool] = True) -> Dict[str, Any]:
        """
        Dumps the scope's content into a dictionary, optionally masking secret values.

        Parameters
        ----------
        mask_secrets : Optional[bool], optional
            Flag to determine whether secret values are masked (default is True).

        Returns
        -------
        Dict[str, Any]
            A dictionary representation of the scope.
        """
        return {
            str(key): "***"
            if isinstance(key, Node) and key.secret and mask_secrets
            else value
            for key, value in self.items()
        } | (
            {"children": [child.dump(mask_secrets) for child in self.children]}
            if self.children
            else {}
        )

    def __str__(self) -> str:  # pragma: no cover
        """
        Converts the scope's content to a JSON-formatted string.

        Returns
        -------
        str
            A JSON string representation of the scope.
        """
        return json.dumps(self.dump(), indent=2)

    def birth(self) -> "Scope":
        """
        Creates a new child scope and attaches it to the current scope.

        Returns
        -------
        Scope
            The newly created child scope.
        """
        child = Scope(self, f"{self.path}/{len(self.children)}")
        self.children.append(child)
        return child

    def __hash__(self) -> int:
        """
        Computes the unique hash value for the scope instance.

        Returns
        -------
        int
            An integer hash value.
        """
        return id(self)

    def dot(
        self,
        filename: str,
        mask_secrets: Optional[bool] = True,
        max_label_size: int = 10,
    ) -> None:  # pragma: no cover
        """
        Generates a DOT graph representation of the scope hierarchy.

        Parameters
        ----------
        filename : str
            The output filename for the DOT file.
        mask_secrets : Optional[bool], optional
            Flag to determine whether secret values are masked (default is True).
        max_label_size : int, optional
            Maximum character length for node labels (default is 10).
        """
        graph = pydot.Dot(graph_type="digraph")

        def add_node(scope):
            data = scope.dump(mask_secrets)
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
