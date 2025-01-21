from typing import Iterable

from .components import Node, Const, Secret, Output, ObjectAccessor


class BlueprintMeta(type):
    """Metaclass to organize blueprint components."""

    def __new__(cls, name, bases, dct):
        nodes, outputs, consts, secrets, objects, edges = {}, {}, {}, {}, {}, []

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
                edges = list(attr_value)

        dct.update(
            {
                "_nodes": nodes,
                "_outputs": outputs,
                "_consts": consts,
                "_secrets": secrets,
                "_objects": objects,
                "_edges": edges,
                "_config_annotations": {
                    name: dct.get("__annotations__", {}).get(name) for name in consts
                },
                "_secret_annotations": {
                    name: dct.get("__annotations__", {}).get(name) for name in secrets
                },
            }
        )

        return super().__new__(cls, name, bases, dct)
