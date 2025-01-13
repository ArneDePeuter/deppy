from typing import Optional, Iterable, TypeVar, Any, Set, Dict, Type
import inspect
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.typing import TSecretValue
import dlt
from dlt.sources import DltResource, DltSource
from dlt.common.configuration.resolve import resolve_configuration

from deppy import Deppy, Scope
from deppy.node import Node


DeppySubclass = TypeVar("DeppySubclass", bound=Deppy)


def create_spec(source_name: str, configs: Set[str], secrets: Set[str]) -> BaseConfiguration:
    annotations: Dict[str, Any] = {}
    defaults: Dict[str, Any] = {}
    for config in configs:
        annotations[config] = Any
        defaults[config] = None
    for secret in secrets:
        annotations[secret] = TSecretValue
        defaults[secret] = None
    cls_dict = {"__annotations__": annotations}
    cls_dict.update(defaults)
    new_class = type(f"Config{source_name}", (BaseConfiguration,), cls_dict)
    return configspec(new_class)  # type: ignore[return-value]


def deppy_to_source(
        deppy: Type[Deppy],
        target_nodes: Optional[Iterable[str]] = None,
        secrets: Optional[Iterable[str]] = None,
        exclude_for_storing: Optional[Iterable[str]] = None,
        with_pbar: Optional[bool] = False
) -> DltSource:
    name = str(deppy.__name__).lower()
    target_nodes = target_nodes or []
    secrets = set(secrets) or set()
    exclude_for_storing = exclude_for_storing or []

    init_param_names = set(inspect.signature(deppy.__init__).parameters.keys())
    init_param_names.remove("self")
    configs = init_param_names - secrets
    spec = create_spec(name, configs, secrets)

    @dlt.source(name=f"{name}_source")
    def source():
        resolved_spec = resolve_configuration(spec(), sections=("sources", name))  # type: ignore[operator]
        init_kwargs = {k: getattr(resolved_spec, k) for k in init_param_names}
        deppy_object = deppy(**init_kwargs)

        actual_target_nodes = set()
        for n in target_nodes:
            node = deppy_object.get_node_by_name(n)
            if node is None:
                raise ValueError(f"Node {n} not found in {deppy_object}")
            actual_target_nodes.add(node)

        @dlt.resource(selected=False, name=f"{name}_extract")
        async def extract() -> DltResource:
            func = deppy_object.execute(*actual_target_nodes, with_pbar=with_pbar)
            if hasattr(deppy, "__aenter__"):
                async with deppy_object:
                    yield await func
            elif hasattr(deppy, "__enter__"):
                with deppy_object:
                    yield await func
            else:
                yield await func

        resources = [extract]
        nodes: list[Node] = deppy_object.graph.nodes if len(target_nodes) == 0 else target_nodes

        actual_exclude_for_storing = set()
        for n in exclude_for_storing:
            node = deppy_object.get_node_by_name(n)
            if node is None:
                raise ValueError(f"Node {n} not found in {deppy_object}")
            actual_exclude_for_storing.add(node)

        nodes = [node for node in nodes if node not in actual_exclude_for_storing]

        for n in nodes:
            if n.secret:
                continue

            @dlt.transformer(data_from=extract, name=n.name)
            async def get_node_data(result: Scope, node: Node = n) -> DltResource:
                yield result.query(node, ignored_results=False)
            resources.append(get_node_data)

        return resources

    return source()
