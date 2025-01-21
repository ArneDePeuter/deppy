import asyncio
from itertools import product
from typing import Any, Tuple, Callable, Iterable, Sequence, Union, Type, Optional

LoopStrategy = Union[Callable[[Sequence[Any]], Iterable[Tuple[Any]]], Type[zip]]


class NodeFunctionError(Exception):
    """
    Exception raised when a Node's function execution fails.

    Attributes
    ----------
    node : Node
        The Node instance where the error occurred.
    """

    def __init__(self, node, *args):
        """
        Constructs a NodeFunctionError.

        Parameters
        ----------
        node : Node
            The node associated with the error.
        *args
            Additional positional arguments for the exception.
        """
        self.node = node
        super().__init__(*args)

    def __str__(self):
        """
        Returns a string representation of the error.

        Returns
        -------
        str
            A string describing the node and the error.
        """
        return f"Error executing node function of {self.node}"


class Node:
    """
    A class representing a computational node.

    Attributes
    ----------
    func : Callable[..., Any]
        The function executed by the node.
    loop_vars : list
        Variables which are looping.
    loop_strategy : Optional[LoopStrategy]
        The strategy used for joining looped variables (default is cartesian product).
    to_thread : Optional[bool]
        Flag to execute the function in a separate thread (default is False). No effect if the function is asynchronous.
    name : Optional[str]
        The name of the node (default is the function's name).
    secret : Optional[bool]
        Indicates whether the node is sensitive and should be masked in outputs (default is False).
    """

    def __init__(
        self,
        func: Callable[..., Any],
        loop_strategy: Optional[LoopStrategy] = product,
        to_thread: Optional[bool] = False,
        name: Optional[str] = None,
        secret: Optional[bool] = False,
    ):
        """
        Constructs a new Node object.

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
        self.func = func
        self.loop_vars = []
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.name = name or func.__name__
        self.secret = secret

    @property
    def is_async(self) -> bool:
        """
        Determines whether the node's function is asynchronous.

        Returns
        -------
        bool
            True if the function is a coroutine function, False otherwise.
        """
        return asyncio.iscoroutinefunction(self.func)

    def call_sync(self, *args, **kwargs):
        """
        Executes the node's function synchronously.

        Parameters
        ----------
        *args
            Positional arguments for the function.
        **kwargs
            Keyword arguments for the function.

        Returns
        -------
        Any
            The result of the function execution.

        Raises
        ------
        NodeFunctionError
            If the function execution fails.
        """
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            raise NodeFunctionError(self, e) from e

    async def call_async(self, *args, **kwargs):
        """
        Executes the node's function asynchronously.

        Parameters
        ----------
        *args
            Positional arguments for the function.
        **kwargs
            Keyword arguments for the function.

        Returns
        -------
        Any
            The result of the asynchronous function execution.

        Raises
        ------
        NodeFunctionError
            If the function execution fails.
        """
        try:
            return await self.func(*args, **kwargs)
        except Exception as e:
            raise NodeFunctionError(self, e) from e

    def __repr__(self):
        """
        Returns a string representation of the node.

        Returns
        -------
        str
            A string in the format <Node name>.
        """
        return f"<Node {self.name}>"

    def __str__(self):
        """
        Returns the node's name.

        Returns
        -------
        str
            The name of the node.
        """
        return self.name
