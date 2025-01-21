from typing import Optional, Iterable, TypeVar, Any, Dict, Type
import inspect
import copy

import dlt
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.extract.source import DltResource, SourceFactory
from dlt.common.configuration.resolve import resolve_configuration

from deppy.blueprint import Node, Blueprint
from deppy.node import Node as DeppyNode

BlueprintSubclass = TypeVar("BlueprintSubclass", bound=Blueprint)


def _create_spec(
    source_name: str,
    configs: Dict[str, type],
    objects: Dict[str, Type[BaseConfiguration]],
) -> Type[BaseConfiguration]:
    """
    Dynamically creates a configuration specification for a source.

    Parameters
    ----------
    source_name : str
        The name of the source.
    configs : Dict[str, type]
        Dictionary of configuration names and their types.
    objects : Dict[str, Type[BaseConfiguration]]
        Dictionary of object names and their configuration specifications.

    Returns
    -------
    Type[BaseConfiguration]
        A dynamically created configuration class.
    """
    annotations: Dict[str, Any] = {}
    defaults: Dict[str, Any] = {}
    for config, type_ in configs.items():
        annotations[config] = type_
        defaults[config] = None
    for object_name, object_spec in objects.items():
        annotations[object_name] = object_spec
        defaults[object_name] = None
    cls_dict = {"__annotations__": annotations}
    cls_dict.update(defaults)
    new_class = type(f"Config{source_name}", (BaseConfiguration,), cls_dict)
    return configspec(new_class)  # type: ignore[return-value]


def _get_object_params(obj: Any) -> Dict[str, type]:
    """
    Extracts the parameters and their types from an object's constructor.

    Parameters
    ----------
    obj : Any
        The object whose parameters are to be extracted.

    Returns
    -------
    Dict[str, type]
        A dictionary mapping parameter names to their types.
    """
    d = inspect.signature(obj.__init__).parameters

    def get_annotation(param):
        return param.annotation if param.annotation != inspect.Parameter.empty else Any

    return {k: get_annotation(v) for k, v in d.items() if k != "self"}


def _create_object_spec(obj_name: str, obj: object) -> Type[BaseConfiguration]:
    """
    Creates a configuration specification for a given object.

    Parameters
    ----------
    obj_name : str
        The name of the object.
    obj : object
        The object for which the specification is created.

    Returns
    -------
    Type[BaseConfiguration]
        A dynamically created configuration class for the object.
    """
    configs = _get_object_params(obj)
    return _create_spec(obj_name, configs, objects={})


def _create_extract_func(deppy: Blueprint, target_nodes: Iterable[Node]) -> Any:
    """
    Creates an extract function for the DLT source based on the given blueprint and target nodes.

    Parameters
    ----------
    deppy : Blueprint
        The blueprint to execute.
    target_nodes : Iterable[Node]
        Nodes to extract data from.

    Returns
    -------
    Any
        An asynchronous or synchronous extraction function.
    """
    async_func = inspect.iscoroutinefunction(deppy.execute)
    async_context = hasattr(deppy, "__aenter__") and hasattr(deppy, "__aexit__")
    sync_context = hasattr(deppy, "__enter__") and hasattr(deppy, "__exit__")

    async def extract_async(
        deppy_=deppy,
        target_nodes_=target_nodes,
        async_context_=async_context,
        sync_context_=sync_context,
    ):
        if async_context_:
            async with deppy_:
                if async_func:
                    yield await deppy_.execute(*target_nodes_)
                else:
                    yield deppy_.execute(*target_nodes_)
        elif sync_context_:
            with deppy:
                yield await deppy_.execute(*target_nodes_)
        else:
            yield await deppy_.execute(*target_nodes_)

    def extract_sync(
        deppy_=deppy, target_nodes_=target_nodes, sync_context_=sync_context
    ):
        if sync_context_:
            with deppy_:
                yield deppy_.execute(*target_nodes_)
        else:
            yield deppy_.execute(*target_nodes_)

    return extract_async if (async_func or async_context) else extract_sync


def blueprint_to_source(
    blueprint: Type[BlueprintSubclass],
    target_nodes: Optional[Iterable[Node]] = None,
    exclude_for_storing: Optional[Iterable[Node]] = None,
    resource_kwargs: Optional[Dict[Node, Dict[str, Any]]] = None,
) -> SourceFactory:
    """
    Converts a Deppy blueprint into a DLT source factory.

    Parameters
    ----------
    blueprint : Type[BlueprintSubclass]
        The blueprint class to convert.
    target_nodes : Optional[Iterable[Node]], optional
        Nodes to include in the source (default is None).
    exclude_for_storing : Optional[Iterable[Node]], optional
        Nodes to exclude from storage (default is None).
    resource_kwargs : Optional[Dict[Node, Dict[str, Any]]], optional
        Additional resource configurations for specific nodes (default is None).

    Returns
    -------
    SourceFactory
        A DLT source factory for the given blueprint.
    """
    name = blueprint.__name__.lower()
    target_nodes = target_nodes or []
    resource_kwargs = resource_kwargs or {}

    exclude_for_storing = exclude_for_storing or []
    configs = copy.deepcopy(blueprint._config_annotations)
    configs.update(blueprint._secret_annotations)
    objects = {
        object_name: _create_object_spec(object_name, object_accesor.type)
        for object_name, object_accesor in blueprint._objects.items()
    }

    spec = _create_spec(name, configs, objects)

    @dlt.source(name=f"{name}_source")
    def source():
        resolved_spec = resolve_configuration(spec(), sections=("sources", name))  # type: ignore[operator]
        init_kwargs = {k: getattr(resolved_spec, k) for k in configs}
        init_kwargs.update(
            {
                obj_name: {
                    param_name: getattr(getattr(resolved_spec, obj_name), param_name)
                    for param_name in _get_object_params(obj)
                }
                for obj_name, obj in objects.items()
            }
        )
        deppy: Blueprint = blueprint(**init_kwargs)

        actual_target_nodes = [deppy.resolve_node(n) for n in target_nodes]
        actual_exclude_for_storing = [
            deppy.resolve_node(n) for n in exclude_for_storing
        ]
        actual_resource_kwargs = {
            deppy.resolve_node(n): kwargs for n, kwargs in resource_kwargs.items()
        }

        extract_func = _create_extract_func(deppy, actual_target_nodes)
        extract = dlt.resource(selected=False, name=f"{name}_extract")(extract_func)

        resources = [extract]
        nodes: list[DeppyNode] = (
            deppy.graph.nodes if len(actual_target_nodes) == 0 else actual_target_nodes
        )
        nodes = [node for node in nodes if node not in actual_exclude_for_storing]

        for n in nodes:
            if n not in actual_target_nodes:
                if n.secret:
                    continue
                if n.name in deppy._consts:
                    continue

            kwargs = actual_resource_kwargs.get(n, {})
            kwargs["data_from"] = extract
            if "name" not in kwargs:
                kwargs["name"] = n.name

            @dlt.transformer(**kwargs)
            def get_node_data(result, node: Node = n) -> DltResource:
                yield from result.query(node, ignored_results=False)

            resources.append(get_node_data)

        return resources

    return source
