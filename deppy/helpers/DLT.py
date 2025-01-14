from typing import Optional, Iterable, TypeVar, Any, Set, Dict, Type
import inspect
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.typing import TSecretValue
import dlt
from dlt.sources import DltResource, DltSource
from dlt.common.configuration.resolve import resolve_configuration

from deppy.blueprint import Node, Blueprint, SecretType
from deppy import Scope
from deppy.node import Node as DeppyNode

BlueprintSubclass = TypeVar("BlueprintSubclass", bound=Blueprint)


def create_spec(source_name: str, configs: Set[str], secrets: Set[str], objects: Dict[str, Type[BaseConfiguration]]) -> Type[BaseConfiguration]:
    annotations: Dict[str, Any] = {}
    defaults: Dict[str, Any] = {}
    for config in configs:
        annotations[config] = Any
        defaults[config] = None
    for secret in secrets:
        annotations[secret] = TSecretValue
        defaults[secret] = None
    for object_name, object_spec in objects.items():
        annotations[object_name] = object_spec
        defaults[object_name] = None
    cls_dict = {"__annotations__": annotations}
    cls_dict.update(defaults)
    new_class = type(f"Config{source_name}", (BaseConfiguration,), cls_dict)
    return configspec(new_class)  # type: ignore[return-value]


def get_object_params(obj: Any) -> Set[str]:
    return set(inspect.signature(obj.__init__).parameters.keys()) - {"self"}


def create_object_spec(obj_name: str, obj: object) -> Type[BaseConfiguration]:
    params = inspect.signature(obj.__init__).parameters
    secrets = set()
    configs = set()
    for k, v in params.items():
        if k == "self":
            continue
        if isinstance(v.annotation, SecretType):
            secrets.add(k)
        else:
            configs.add(k)
    return create_spec(obj_name, configs, secrets, {})


def blueprint_to_source(
        blueprint: Type[BlueprintSubclass],
        target_nodes: Optional[Iterable[Node]] = None,
        exclude_for_storing: Optional[Iterable[Node]] = None,
        with_pbar: Optional[bool] = False
) -> DltSource:
    name = blueprint.__name__
    target_nodes = target_nodes or []

    exclude_for_storing = exclude_for_storing or []
    secrets = set(blueprint._secrets.keys())
    configs = set(blueprint._consts.keys())
    objects = {
        object_name: create_object_spec(object_name, object_accesor.type)
        for object_name, object_accesor in blueprint._objects.items()
    }

    spec = create_spec(name, configs, secrets, objects)

    @dlt.source(name=f"{name}_source")
    def source():
        resolved_spec = resolve_configuration(spec(), sections=("sources", name))  # type: ignore[operator]
        init_kwargs = {k: getattr(resolved_spec, k) for k in (configs | secrets)}
        init_kwargs.update({
            obj_name: {
                param_name: getattr(getattr(resolved_spec, obj_name), param_name)
                for param_name in get_object_params(obj)
            } for obj_name, obj in objects.items()
        })
        deppy: Blueprint = blueprint(**init_kwargs)

        actual_target_nodes = [deppy.bp_to_node_map[n] for n in target_nodes]
        actual_exclude_for_storing = [deppy.bp_to_node_map[n] for n in exclude_for_storing]

        @dlt.resource(selected=False, name=f"{deppy._name}_extract")
        async def extract() -> DltResource:
            func = deppy.execute(*actual_target_nodes, with_pbar=with_pbar)
            if hasattr(deppy, "__aenter__"):
                async with deppy:
                    yield await func
            elif hasattr(deppy, "__enter__"):
                with deppy:
                    yield await func
            else:
                yield await func

        resources = [extract]
        nodes: list[DeppyNode] = deppy.graph.nodes if len(actual_target_nodes) == 0 else actual_target_nodes
        nodes = [node for node in nodes if node not in actual_exclude_for_storing]

        for n in nodes:
            if n not in actual_target_nodes:
                if n.secret:
                    continue
                if n.name in deppy._consts:
                    continue

            @dlt.transformer(data_from=extract, name=n.name)
            async def get_node_data(result: Scope, node: Node = n) -> DltResource:
                yield result.query(node, ignored_results=False)
            resources.append(get_node_data)

        return resources

    return source()
