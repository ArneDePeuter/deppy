from typing import Any, Optional, Iterable, Callable, TypeVar, Type, ParamSpec
from ..node import LoopStrategy, product


T = TypeVar("T")
P = ParamSpec("P")


class ObjectAccessor:
    """Helper for managing object attribute accesses dynamically."""

    def __init__(self, obj_type: Type[T]):
        self.type = obj_type
        self.curr_access: list[str] = []
        self.name: Optional[str] = None

    def __getattr__(self, item: str) -> "ObjectAccessor":
        self.curr_access.append(item)
        return self

    def __prune__(self) -> list[str]:
        """Prune current access path."""
        path = self.curr_access
        self.curr_access = []
        return path


def Object(t: Type[T]) -> ObjectAccessor:
    """Factory to create an ObjectAccessor for the given type."""
    return ObjectAccessor(t)


class BlueprintObject:
    """Base class for blueprint components."""


class Input:
    """Represents an input for a Node."""

    def __init__(
        self, from_node: BlueprintObject, name: Optional[str] = None, loop: bool = False
    ):
        self.from_node = from_node
        self.name = name
        self.loop = loop


class Node(BlueprintObject):
    """Represents a computational node in a blueprint."""

    def __init__(
        self,
        func: Callable[..., Any],
        loop_strategy: Optional[LoopStrategy] = product,
        to_thread: bool = False,
        name: Optional[str] = None,
        secret: bool = False,
        inputs: Optional[Iterable[Any]] = None,
    ):
        if isinstance(func, ObjectAccessor):
            self.accesses = func.__prune__()
        else:
            self.accesses = []

        self.func = func
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.name = name or func.__name__
        self.secret = secret
        self.inputs = inputs or []

    def __repr__(self):  # pragma: no cover
        return f"<Node {self.name}>"

    def __str__(self):  # pragma: no cover
        return self.name


class Output(BlueprintObject):
    """Represents an output from a node."""

    def __init__(
        self,
        node: BlueprintObject,
        extractor: Callable[[Any], Any] = lambda x: x,
        loop: bool = False,
        secret: Optional[bool] = None,
    ):
        self.node = node
        self.extractor = extractor
        self.loop = loop
        self.secret = secret


class Const(BlueprintObject):
    """Represents a constant value in a blueprint."""

    def __init__(self, value: Any = None):
        self.value = value


class Secret(BlueprintObject):
    """Represents a secret value in a blueprint."""

    def __init__(self, value: Any = None):
        self.value = value
