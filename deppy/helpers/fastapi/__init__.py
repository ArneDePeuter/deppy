from fastapi import FastAPI
from deppy.blueprint import Blueprint
from typing import Type

from .websocket import create_websocket_endpoint
from .endpoints import create_endpoints


def create_app(blueprint: Type[Blueprint]) -> FastAPI:
    """
    Creates a FastAPI application for given blueprint.

    Parameters
    ----------
    blueprint : Blueprint
        The blueprint instance.

    Returns
    -------
    FastAPI
        The FastAPI application instance.
    """
    app = FastAPI(title=blueprint.__name__)
    create_websocket_endpoint(app, blueprint)
    create_endpoints(app, blueprint)
    return app
