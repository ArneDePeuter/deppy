from .websocket import create_websocket_endpoint
from fastapi import FastAPI
from deppy.blueprint import Blueprint
from typing import Type


def create_api(blueprint: Type[Blueprint]) -> FastAPI:
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
    app = FastAPI()
    create_websocket_endpoint(app, blueprint)
    return app
