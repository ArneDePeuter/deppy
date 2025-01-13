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
        deppy: Deppy,
        target_nodes: Optional[Iterable[Node]] = None,
        exclude_for_storing: Optional[Iterable[Node]] = None,
        with_pbar: Optional[bool] = False
) -> DltSource:
    name = deppy.name.lower()
    target_nodes = target_nodes or []
    exclude_for_storing = exclude_for_storing or []
    secrets = set(deppy.graph_builder.secrets.keys())

    configure_param_names = set(inspect.signature(deppy.configure).parameters.keys())
    configure_param_names.remove("kwargs")
    configs = configure_param_names | set(deppy.graph_builder.consts.keys())
    spec = create_spec(name, configs, secrets)

    @dlt.source(name=f"{name}_source")
    def source():
        resolved_spec = resolve_configuration(spec(), sections=("sources", name))  # type: ignore[operator]
        configure_kwargs = {k: getattr(resolved_spec, k) for k in (configs | secrets)}
        deppy.configure(**configure_kwargs)

        @dlt.resource(selected=False, name=f"{name}_extract")
        async def extract() -> DltResource:
            func = deppy.execute(*target_nodes, with_pbar=with_pbar)
            if hasattr(deppy, "__aenter__"):
                async with deppy:
                    yield await func
            elif hasattr(deppy, "__enter__"):
                with deppy:
                    yield await func
            else:
                yield await func

        resources = [extract]
        nodes: list[Node] = deppy.graph.nodes if len(target_nodes) == 0 else target_nodes
        nodes = [node for node in nodes if node not in exclude_for_storing]

        for n in nodes:
            if n.secret:
                continue
            if n.name in deppy.consts:
                continue

            @dlt.transformer(data_from=extract, name=n.name)
            async def get_node_data(result: Scope, node: Node = n) -> DltResource:
                yield result.query(node, ignored_results=False)
            resources.append(get_node_data)

        return resources

    return source()
