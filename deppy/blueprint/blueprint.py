from typing import Union

from .meta import BlueprintMeta
from .components import (
    ObjectAccessor,
    Node,
    Const,
    Secret,
    Output,
    BlueprintObject,
    Input,
)
from ..node import Node as DeppyNode
from ..deppy import Deppy


class Blueprint(Deppy, metaclass=BlueprintMeta):
    """Blueprint definition."""

    _objects: dict[str, ObjectAccessor]
    _nodes: dict[str, Node]
    _outputs: dict[str, Output]
    _consts: dict[str, Const]
    _secrets: dict[str, Secret]
    _edges: list[tuple[BlueprintObject, BlueprintObject, str]]

    def resolve_node(self, node: BlueprintObject) -> DeppyNode:
        """Resolve a blueprint object to its corresponding DeppyNode."""
        actual_node = self.bp_to_node_map.get(node)
        if not actual_node:
            raise ValueError(f"Node '{node}' not found in blueprint.")
        return actual_node

    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__)
        self.bp_to_node_map = {}
        object_map = self._initialize_objects(kwargs)
        self._initialize_nodes(object_map)
        self._initialize_outputs()
        self._initialize_consts(kwargs)
        self._initialize_secrets(kwargs)
        self._initialize_edges()
        self._setup_context_managers(object_map)

    def _initialize_objects(self, kwargs: dict) -> dict:
        """Initialize objects defined in the blueprint."""
        object_map = {}
        for name, obj in self._objects.items():
            input_ = kwargs.get(name, {})
            if isinstance(input_, dict):
                obj_instance = obj.type(**input_)
            elif isinstance(input_, obj.type):
                obj_instance = input_
            else:
                raise ValueError(f"Invalid input for object '{name}'")
            object_map[name] = obj_instance
            setattr(self, name, obj_instance)
        return object_map

    def _initialize_nodes(self, object_map: dict):
        """Initialize nodes in the blueprint."""
        for name, bp_node in self._nodes.items():
            func = bp_node.func
            if isinstance(func, ObjectAccessor):
                obj = object_map[bp_node.func.name]
                for access in bp_node.accesses:
                    obj = getattr(obj, access)
                func = obj

            node = DeppyNode(
                func=func,
                loop_strategy=bp_node.loop_strategy,
                to_thread=bp_node.to_thread,
                name=name,
                secret=bp_node.secret,
            )

            self.bp_to_node_map[bp_node] = node
            self.graph.add_node(node)
            setattr(self, name, node)

    def _initialize_outputs(self):
        """Initialize outputs in the blueprint."""
        for name, output in self._outputs.items():
            actual_node = self.resolve_node(output.node)
            output_obj = self.add_output(
                node=actual_node,
                name=name,
                extractor=output.extractor,
                loop=output.loop,
                secret=output.secret,
            )
            self.bp_to_node_map[output] = output_obj
            setattr(self, name, output_obj)

    def _initialize_consts(self, kwargs: dict):
        """Initialize constants in the blueprint."""
        for name, const in self._consts.items():
            const_obj = self.add_const(const.value or kwargs.get(name), name)
            self.bp_to_node_map[const] = const_obj
            setattr(self, name, const_obj)

    def _initialize_secrets(self, kwargs: dict):
        """Initialize secrets in the blueprint."""
        for name, secret in self._secrets.items():
            secret_obj = self.add_secret(secret.value or kwargs.get(name), name)
            self.bp_to_node_map[secret] = secret_obj
            setattr(self, name, secret_obj)

    def _initialize_edges(self):
        """Initialize edges in the blueprint."""
        for edge in self._edges:
            assert len(edge) == 3, "Edges must be tuples with at least 3 elements"
            u = self.resolve_node(edge[0])
            v = self.resolve_node(edge[1])
            self.add_edge(u, v, *edge[2:])

        for node in self._nodes.values():
            actual_node = self.resolve_node(node)
            for input_ in node.inputs:
                self._add_edge_for_input(input_, actual_node)

    def _add_edge_for_input(
        self, input_: Union[Input, BlueprintObject], actual_node: DeppyNode
    ):
        """Add edges based on the node inputs."""
        if isinstance(input_, Input):
            from_node = self.resolve_node(input_.from_node)
            input_name = input_.name or from_node.name
            self.add_edge(from_node, actual_node, input_name, input_.loop)
        elif isinstance(input_, BlueprintObject):
            from_node = self.resolve_node(input_)
            self.add_edge(from_node, actual_node, from_node.name, False)
        else:
            raise ValueError(
                f"Invalid input {input_} for node '{actual_node}'. Must be Input or BlueprintObject"
            )

    def _setup_context_managers(self, object_map: dict):
        """Set up context manager methods (__enter__, __exit__, __aenter__, __aexit__)."""
        async_context_manager = any(
            hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__")
            for obj in object_map.values()
        )
        sync_context_manager = any(
            hasattr(obj, "__enter__") and hasattr(obj, "__exit__")
            for obj in object_map.values()
        )

        if async_context_manager:

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

        elif sync_context_manager:

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
