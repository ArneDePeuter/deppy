from typing import Any, Optional, Iterable, Callable, TypeVar, Type

from .node import Node
from .deppy import Deppy


T = TypeVar("T")


class Output:
    def __init__(self, node: Node, extractor: Optional[Callable[[Any], Any]], loop: Optional[bool] = False, secret: Optional[bool] = None):
        self.node = node
        self.extractor = extractor
        self.loop = loop
        self.secret = secret


class ObjectAccessor:
    def __init__(self, t):
        self.type = t
        self.accesses_methods = []
        self.name = None

    def __getattr__(self, item):
        self.accesses_methods.append(item)
        return self


def Object(t: Type[T]) -> T:
    return ObjectAccessor(t)


class Const:
    def __init__(self, value: Optional[Any] = None):
        self.value = value


class Secret:
    def __init__(self, value: Optional[Any] = None):
        self.value = value


class DeppyBlueprintMeta(type):
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

        dct["_nodes"] = nodes
        dct["_consts"] = consts
        dct["_secrets"] = secrets
        dct["_edges"] = edges
        dct["_outputs"] = outputs
        dct["_objects"] = objects

        return super().__new__(cls, name, bases, dct)


class DeppyBlueprint(Deppy, metaclass=DeppyBlueprintMeta):
    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__)

        object_map = {}
        bp_to_node_map = {}

        for name, obj in self._objects.items():
            obj = obj.type(**(kwargs.get(name) or {}))
            object_map[name] = obj
            setattr(self, name, obj)

        for name, node in self._nodes.items():
            if isinstance(node.func, ObjectAccessor):
                obj = object_map[node.func.name]
                accessing = True
                while accessing:
                    method = node.func.accesses_methods.pop(0)
                    # because this gets called firstly on the function in the Node constructor
                    if method == "__name__":
                        accessing = False
                        continue
                    obj = getattr(obj, method)
                node.func = obj
            node.name = name
            bp_to_node_map[node] = node # these are always nodes and not blueprints
            self.graph.add_node(node)

        for name, output in self._outputs.items():
            bp = output
            output = self.add_output(output.node, name, output.extractor, output.loop, output.secret)
            bp_to_node_map[bp] = output
            setattr(self, name, output)

        for name, const in self._consts.items():
            bp = const
            const = self.add_const(name, const.value or kwargs.get(name))
            bp_to_node_map[bp] = const
            setattr(self, name, const)

        for name, secret in self._secrets.items():
            bp = secret
            secret = self.add_secret(name, secret.value or kwargs.get(name))
            bp_to_node_map[bp] = secret
            setattr(self, name, secret)

        for edge in self._edges:
            assert len(edge) == 3, "Edges must be tuples with min length of 3"

            u = bp_to_node_map[edge[0]]
            v = bp_to_node_map[edge[1]]

            self.add_edge(u, v, *(edge[2:]))

        async_context_mngr = False
        for obj in object_map.values():
            if hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__"):
                async_context_mngr = True
                break

        if async_context_mngr:
            async def __aenter__(self):
                for obj in object_map.values():
                    if hasattr(obj, "__aenter__"):
                        await obj.__aenter__()
                    else:
                        await obj.__enter__()
                return self

            async def __aexit__(self, exc_type, exc_value, traceback):
                for obj in object_map.values():
                    if hasattr(obj, "__aexit__"):
                        await obj.__aexit__(exc_type, exc_value, traceback)
                    else:
                        await obj.__exit__(exc_type, exc_value, traceback)

            setattr(self.__class__, "__aenter__", __aenter__)
            setattr(self.__class__, "__aexit__", __aexit__)
        else:
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

