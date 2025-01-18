from typing import Any, Optional, Iterable, Callable, TypeVar, Type, ParamSpec, Union

from .node import Node as DeppyNode, LoopStrategy, product
from .deppy import Deppy


T = TypeVar("T")

P = ParamSpec("P")
FT = TypeVar("FT")


class ObjectAccessor:
    def __init__(self, t):
        self.type = t
        self.curr_access = []
        self.name = None

    def __getattr__(self, item):
        if item == "*":
            cur = self.curr_access
            self.curr_access = []
            return cur
        self.curr_access.append(item)
        return self

    def reset(self):
        self.curr_access = []


def Object(t: Type[T]) -> T:
    return ObjectAccessor(t)


class Input:
    def __init__(self, from_node: Any, name: Optional[str] = None, loop: Optional[bool] = False):
        self.from_node = from_node
        self.name = name
        self.loop = loop

class BlueprintObject:
    pass


class Node(BlueprintObject):
    def __init__(
        self,
        func: Callable[..., Any],
        loop_strategy: Optional[LoopStrategy] = product,
        to_thread: Optional[bool] = False,
        name: Optional[str] = None,
        secret: Optional[bool] = False,
        inputs: Optional[Iterable[Any]] = None,
    ):
        if isinstance(func, ObjectAccessor):
            self.accesses = func.__getattr__("*")
        else:
            self.accesses = []
        self.func = func
        self.loop_strategy = loop_strategy
        self.to_thread = to_thread
        self.name = name or func.__name__
        self.secret = secret
        self.inputs = inputs or []
        if isinstance(func, ObjectAccessor):
            # reset because we called __name__
            func.reset()

    def __repr__(self):  # pragma: no cover
        return f"<Node {self.name}>"

    def __str__(self):  # pragma: no cover
        return self.name


class Output(BlueprintObject):
    def __init__(
        self,
        node: Node,
        extractor: Optional[Callable[[Any], Any]] = lambda x: x,
        loop: Optional[bool] = False,
        secret: Optional[bool] = None,
    ):
        self.node = node
        self.extractor = extractor
        self.loop = loop
        self.secret = secret


class Const(BlueprintObject):
    def __init__(self, value: Optional[Any] = None):
        self.value = value


class Secret(BlueprintObject):
    def __init__(self, value: Optional[Any] = None):
        self.value = value


class BlueprintMeta(type):
    def __new__(cls, name, bases, dct):
        nodes = {}
        outputs = {}
        consts = {}
        secrets = {}
        objects = {}
        edges = []

        for attr_name, attr_value in dct.items():
            if isinstance(attr_value, Node):
                nodes[attr_name] = attr_value
            elif isinstance(attr_value, Const):
                consts[attr_name] = attr_value
            elif isinstance(attr_value, Secret):
                secrets[attr_name] = attr_value
            elif isinstance(attr_value, Output):
                outputs[attr_name] = attr_value
            elif isinstance(attr_value, ObjectAccessor):
                objects[attr_name] = attr_value
                attr_value.name = attr_name
            elif attr_name == "edges" and isinstance(attr_value, Iterable):
                edges = attr_value

        type_annotations = dct.get("__annotations__", {})
        config_annotations = {name: type_annotations.get(name) for name in consts}
        secret_annotations = {name: type_annotations.get(name) for name in secrets}

        dct["_nodes"] = nodes
        dct["_consts"] = consts
        dct["_secrets"] = secrets
        dct["_edges"] = edges
        dct["_outputs"] = outputs
        dct["_objects"] = objects
        dct["_config_annotations"] = config_annotations
        dct["_secret_annotations"] = secret_annotations

        return super().__new__(cls, name, bases, dct)


class Blueprint(Deppy, metaclass=BlueprintMeta):
    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__)

        object_map = {}
        self.bp_to_node_map = {}

        for name, obj in self._objects.items():
            input_ = kwargs.get(name, {})
            if isinstance(input_, dict):
                obj = obj.type(**input_)
            elif isinstance(input_, obj.type):
                obj = input_
            else:
                raise ValueError(f"Invalid input for object '{name}'")
            object_map[name] = obj
            setattr(self, name, obj)

        for name, bp in self._nodes.items():
            node = DeppyNode(bp.func, bp.loop_strategy, bp.to_thread, name, bp.secret)
            if isinstance(bp.func, ObjectAccessor):
                obj = object_map[bp.func.name]
                for access in bp.accesses:
                    obj = getattr(obj, access)
                node.func = obj
            self.bp_to_node_map[bp] = node
            self.graph.add_node(node)
            setattr(self, name, node)

        for name, output in self._outputs.items():
            bp = output
            actual_node = self.bp_to_node_map[output.node]
            output = self.add_output(
                actual_node, name, output.extractor, output.loop, output.secret
            )
            self.bp_to_node_map[bp] = output
            setattr(self, name, output)

        for name, const in self._consts.items():
            bp = const
            const = self.add_const(const.value or kwargs.get(name), name)
            self.bp_to_node_map[bp] = const
            setattr(self, name, const)

        for name, secret in self._secrets.items():
            bp = secret
            secret = self.add_secret(secret.value or kwargs.get(name), name)
            self.bp_to_node_map[bp] = secret
            setattr(self, name, secret)

        for edge in self._edges:
            assert len(edge) == 3, "Edges must be tuples with min length of 3"

            u = self.bp_to_node_map[edge[0]]
            v = self.bp_to_node_map[edge[1]]

            self.add_edge(u, v, *(edge[2:]))

        for node in self._nodes.values():
            actual_node = self.bp_to_node_map[node]
            for input_ in node.inputs:
                if isinstance(input_, Input):
                    from_node = resolve_node(self, input_.from_node)
                    input_name = input_.name or from_node.name
                    self.add_edge(from_node, actual_node, input_name, input_.loop)
                elif isinstance(input_, BlueprintObject):
                    from_node = resolve_node(self, input_)
                    self.add_edge(from_node, actual_node, from_node.name, False)
                else:
                    raise ValueError(f"Invalid input {input_} for node '{node}'. It must be Input or BlueprintObject")

        async_context_mngr = False
        sync_context_mngr = False
        for obj in object_map.values():
            if hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__"):
                async_context_mngr = True
                break
            if hasattr(obj, "__enter__") and hasattr(obj, "__exit__"):
                sync_context_mngr = True

        if async_context_mngr:

            async def __aenter__(self):
                for obj in object_map.values():
                    if hasattr(obj, "__aenter__"):
                        await obj.__aenter__()
                    elif hasattr(obj, "__enter__"):
                        obj.__enter__()
                return self

            async def __aexit__(self, exc_type, exc_value, traceback):
                for obj in object_map.values():
                    if hasattr(obj, "__aexit__"):
                        await obj.__aexit__(exc_type, exc_value, traceback)
                    elif hasattr(obj, "__exit__"):
                        obj.__exit__(exc_type, exc_value, traceback)

            setattr(self.__class__, "__aenter__", __aenter__)
            setattr(self.__class__, "__aexit__", __aexit__)
        elif sync_context_mngr:

            def __enter__(self):
                for obj in object_map.values():
                    if hasattr(obj, "__enter__"):
                        obj.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                for obj in object_map.values():
                    if hasattr(obj, "__exit__"):
                        obj.__exit__(exc_type, exc_value, traceback)

            setattr(self.__class__, "__enter__", __enter__)
            setattr(self.__class__, "__exit__", __exit__)


def resolve_node(blueprint: Blueprint, node: BlueprintObject) -> DeppyNode:
    actual_node = blueprint.bp_to_node_map.get(node)
    if actual_node is None:
        raise ValueError(f"Node '{node}' not found in blueprint")
    return actual_node
