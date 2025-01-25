from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, conlist, create_model
from typing import Type, Any, Dict, Optional
from deppy.blueprint import Blueprint
from deppy.blueprint.blueprint import BlueprintObject
import inspect


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


def create_object_basemodel(obj_name: str, obj: object) -> Type[BaseModel]:
    """
    Creates a Pydantic BaseModel for an object based on its parameters.

    Parameters
    ----------
    obj_name : str
        The name of the object.
    obj : object
        The object whose parameters are to be extracted.

    Returns
    -------
    Type[BaseModel]
        A dynamically created Pydantic BaseModel for the object.
    """
    configs = _get_object_params(obj)
    return create_model(obj_name, **{param: (param_type, ...) for param, param_type in configs.items()})


def create_body_basemodel(blueprint: Type[Blueprint], with_target_nodes: Optional[bool] = False) -> Type[BaseModel]:
    """
    Dynamically creates a Pydantic BaseModel based on the blueprint's configuration annotations,
    secret annotations, and objects.

    Parameters
    ----------
    blueprint : Type[Blueprint]
        The blueprint to create a Pydantic BaseModel for.

    with_target_nodes : Optional[bool]
        Whether to include the target nodes in the Pydantic BaseModel.

    Returns
    -------
    Type[BaseModel]
        The dynamically created Pydantic BaseModel.
    """
    annotations = {}

    for param_name, param_type in blueprint._config_annotations.items():
        annotations[param_name] = param_type

    for param_name, param_type in blueprint._secret_annotations.items():
        annotations[param_name] = param_type

    for obj_name, object_accesor in blueprint._objects.items():
        obj_model = create_object_basemodel(obj_name, object_accesor.type)
        annotations[obj_name] = obj_model

    if with_target_nodes:
        annotations["target_nodes"] = conlist(str, min_length=1)

    return create_model("BodyModel", **{param: (param_type, ...) for param, param_type in annotations.items()})


def create_bp_endpoint(
        app: FastAPI,
        body_model: Type[BaseModel],
        blueprint: Type[Blueprint],
        node_name: str,
        bp: BlueprintObject
):
    """
    Dynamically creates a FastAPI endpoint for a given blueprint node.

    Parameters
    ----------
    app : FastAPI
        The FastAPI application instance.
    body_model : Type[BaseModel]
        The Pydantic BaseModel for the request body.
    blueprint : Type[Blueprint]
        The blueprint to create the endpoint for.
    node_name : str
        The name of the node.
    bp : BlueprintObject
        The blueprint object to create the endpoint for.
    """
    @app.post(f"/{node_name}")
    async def node_endpoint(body: body_model):
        try:
            init_kwargs = body.dict()
            deppy_instance = blueprint(**init_kwargs)
            actual_node = deppy_instance.resolve_node(bp)
            result = deppy_instance.execute(actual_node)
            return JSONResponse(content={node_name: result.query(actual_node)})
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))


def create_endpoints(
        app: FastAPI,
        blueprint: Type[Blueprint]
) -> None:
    """
    Dynamically creates FastAPI endpoints based on the given blueprint.

    Parameters
    ----------
    app : FastAPI
        The FastAPI application instance.
    blueprint : Blueprint
        The blueprint to create endpoints for.
    """
    BodyModel = create_body_basemodel(blueprint)

    for name, node in blueprint._nodes.items():
        create_bp_endpoint(app, BodyModel, blueprint, name, node)

    for name, output in blueprint._outputs.items():
        create_bp_endpoint(app, BodyModel, blueprint, name, output)

    BodyModelWithTargetNodes = create_body_basemodel(blueprint, with_target_nodes=True)

    @app.post("/execute")
    async def execute_endpoint(body: BodyModelWithTargetNodes):
        try:
            init_kwargs = body.dict()
            deppy_instance = blueprint(**init_kwargs)
            actual_nodes = []
            for node_name in body.target_nodes:
                actual_node = deppy_instance.get_node_by_name(node_name)
                if actual_node is None:
                    raise ValueError(f"Node '{node_name}' not found in blueprint.")
                actual_nodes.append(deppy_instance.get_node_by_name(node_name))
            result = deppy_instance.execute(*actual_nodes)
            return JSONResponse(content={node.name: result.query(node) for node in actual_nodes})
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

