from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from deppy.blueprint import Blueprint
from typing import Type, Optional
from deppy import Scope, Deppy


def create_websocket_endpoint(
    app: FastAPI,
    blueprint: Type[Blueprint]
) -> None:
    """
    Creates a WebSocket endpoint for interacting with Deppy blueprint execution.

    This function registers a WebSocket endpoint in the provided FastAPI application instance.
    The WebSocket connection allows the client to configure, execute, and query a Deppy instance
    through different message types.

    Parameters
    ----------
    app : fastapi.FastAPI
        The FastAPI application instance where the WebSocket endpoint will be added.
    blueprint : Type[Blueprint]
        The Deppy blueprint class to be used for creating and executing nodes.
    """
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """
        Handles WebSocket communication for configuring, executing, and querying Deppy.

        This function processes incoming WebSocket messages and determines the type of request (configure, execute, or get).
        Based on the request, it delegates to appropriate helper functions to configure the Deppy instance,
        execute tasks, or retrieve results.

        Parameters
        ----------
        websocket : WebSocket
            The WebSocket connection to the client, through which messages are exchanged.
        """
        await websocket.accept()
        deppy: Optional[Deppy] = None
        scope: Optional[Scope] = None

        try:
            while True:
                payload = await websocket.receive_json()
                request_type = payload.get("type")

                if request_type == "configure":
                    deppy = await handle_configure(websocket, blueprint, payload)

                elif request_type == "execute":
                    scope = await handle_execute(websocket, deppy, payload)

                elif request_type == "get":
                    await handle_get(websocket, deppy, scope, payload)

        except WebSocketDisconnect:
            await websocket.close()


async def handle_configure(websocket: WebSocket, blueprint: Type[Blueprint], payload: dict) -> Optional[Deppy]:
    """
    Configures the Deppy instance with the provided configuration data.

    This function initializes the Deppy instance using the provided data in the WebSocket payload.
    If successful, it returns the Deppy instance. In case of an error, it sends an error message to the client.

    Parameters
    ----------
    websocket : WebSocket
        The WebSocket connection to the client.
    blueprint : Type[Blueprint]
        The Deppy blueprint class to initialize.
    payload : dict
        The payload containing configuration data to be passed to the blueprint.

    Returns
    -------
    Optional[Deppy]
        The configured Deppy instance if successful, or `None` if there was an error.
    """
    try:
        deppy = blueprint(**payload.get("data", {}))
        await websocket.send_json({"status": "success"})
        return deppy
    except Exception as e:
        await websocket.send_json({"error": str(e)})


async def handle_execute(websocket: WebSocket, deppy: Optional[Deppy], payload: dict) -> Optional[Scope]:
    """
    Executes the Deppy blueprint for the target nodes.

    This function triggers the execution of specified nodes in the Deppy blueprint based on the provided target nodes.
    It handles both synchronous and asynchronous execution, sending a success response once execution is complete.

    Parameters
    ----------
    websocket : WebSocket
        The WebSocket connection to the client.
    deppy : Optional[Deppy]
        The Deppy instance that was configured.
    payload : dict
        The payload containing the target nodes to execute.

    Returns
    -------
    Optional[Scope]
        The scope of the execution, which is used to retrieve the results of the execution.
        If there is an error, an error message is sent to the client.
    """
    if deppy is None:
        await websocket.send_json({"error": "Deppy not configured."})
        return

    target_nodes = payload.get("data", [])
    try:
        target_nodes = [deppy.get_node_by_name(node) for node in target_nodes]
        if deppy.execute_is_async():
            result = await deppy.execute(*target_nodes)
        else:
            result = deppy.execute(*target_nodes)
        await websocket.send_json({"status": "success"})
        return result
    except Exception as e:
        await websocket.send_json({"error": str(e)})


async def handle_get(websocket: WebSocket, deppy: Optional[Deppy], scope: Optional[Scope], payload: dict) -> None:
    """
    Retrieves the results for the requested target node.

    This function queries the scope for the result of the specified target node. If the Deppy instance
    or the scope is not available, it sends an error message. Otherwise, it sends the result back to the client.

    Parameters
    ----------
    websocket : WebSocket
        The WebSocket connection to the client.
    deppy : Optional[Deppy]
        The Deppy instance (should be configured beforehand).
    scope : Optional[Scope]
        The scope of the current execution, which holds the results of the executed nodes.
    payload : dict
        The payload containing the target node for querying.

    Returns
    -------
    None
        Sends a response to the client with the result of the query or an error message.
    """
    if deppy is None:
        await websocket.send_json({"error": "Deppy not configured."})
        return

    if scope is None:
        await websocket.send_json({"error": "No result available."})
        return

    target_node = payload.get("data")
    try:
        target_node = deppy.get_node_by_name(target_node)
        result = scope.query(target_node)
        await websocket.send_json({"status": "success", "data": result})
    except Exception as e:
        await websocket.send_json({"error": str(e)})
