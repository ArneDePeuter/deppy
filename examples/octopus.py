from typing import Callable, TypeVar, ParamSpec, Union, Any, Awaitable, Iterable
from functools import wraps
import httpx
import os
import json
import asyncio
from deppy import Deppy, IgnoreResult
from deppy.wrappers.dkr import Dkr, JsonDk, StringDk
from deppy.wrappers.stated_kwargs import StatedKwargs
from datetime import datetime

P = ParamSpec("P")
T = TypeVar("T")


def request_wrapper(function: Callable[P, Awaitable[httpx.Response]]) -> Callable[P, Union[IgnoreResult, Any]]:
    @wraps(function)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Union[IgnoreResult, Any]:
        result = await function(*args, **kwargs)
        result.raise_for_status()
        return result.json()

    return wrapper


def ignore_on_status_codes(function: Callable[P, Awaitable[httpx.Response]], status_codes: Iterable[int]) -> Callable[P, Union[IgnoreResult, Any]]:
    @wraps(function)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Union[IgnoreResult, Any]:
        try:
            result = await function(*args, **kwargs)
            return result.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code in status_codes:
                return IgnoreResult()
            raise

    return wrapper


def octopus(
        client: httpx.AsyncClient,
        software_house_uuid: str,
        user: str,
        password: str,
        locale_id: int,
        initial_modified_timestamp: str,
        state: StatedKwargs
) -> Deppy:
    client.request = request_wrapper(client.request)

    # create all the requests
    auth_request = Dkr(
        url="/authentication",
        headers=JsonDk({"softwareHouseUuid": "{software_house_uuid}"}),
        json=JsonDk({"user": "{user}", "password": "{password}"}),
    )(client.post, "auth")

    dossiers_request = Dkr(
        url="/dossiers",
        headers=JsonDk({"Token": "{token}"})
    )(client.get, "dossiers")

    dossier_token_info_request = Dkr(
        url="/dossiers",
        headers=JsonDk({"Token": "{token}"}),
        params=JsonDk({"dossierId": "{dossier_id}", "localeId": "{locale_id}"})
    )(client.post, "dossier_token_info")

    modified_accounts_request = ignore_on_status_codes(
        state.stated_kwarg(
            name="modified_timestamp",
            initial_value=initial_modified_timestamp,
            produce_function=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            keys=["dossier_id"],
            func=Dkr(
                url=StringDk("/dossiers/{dossier_id}/accounts/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
            )(client.get, "modified_accounts")
        ),
        {404}
    )

    bookyears_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/bookyears"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "bookyears")

    # create the Deppy graph
    deppy = Deppy()

    # create all constants/secrets
    software_house_uuid_node = deppy.add_secret(software_house_uuid, name="software_house_uuid")
    user_node = deppy.add_secret(user, name="user")
    password_node = deppy.add_secret(password, name="password")
    locale_id_node = deppy.add_const(locale_id, name="locale_id")

    # create all nodes
    auth_node = deppy.add_node(auth_request, secret=True)
    token_node = deppy.add_output(auth_node, "token", lambda data: data["token"])

    dossiers_node = deppy.add_node(dossiers_request)
    dossier_id_node = deppy.add_output(dossiers_node, "dossier_id", lambda dossier: dossier["dossierKey"]["id"], loop=True)

    dossier_token_info_node = deppy.add_node(dossier_token_info_request, secret=True)
    dossier_token_node = deppy.add_output(dossier_token_info_node, "dossier_token", lambda data: data["Dossiertoken"])

    bookyears_node = deppy.add_node(bookyears_request)

    # create all edges
    deppy.add_edge(software_house_uuid_node, auth_node, "software_house_uuid")
    deppy.add_edge(user_node, auth_node, "user")
    deppy.add_edge(password_node, auth_node, "password")
    deppy.add_edge(token_node, dossiers_node, "token")
    deppy.add_edge(dossier_id_node, dossier_token_info_node, "dossier_id")
    deppy.add_edge(token_node, dossier_token_info_node, "token")
    deppy.add_edge(locale_id_node, dossier_token_info_node, "locale_id")
    deppy.add_edge(dossier_token_node, bookyears_node, "dossier_token")
    deppy.add_edge(dossier_id_node, bookyears_node, "dossier_id")

    return deppy


async def main():
    async with httpx.AsyncClient(base_url="https://service.inaras.be/octopus-rest-api/v1") as client:
        deppy = octopus(
            client,
            os.getenv("SOFTWARE_HOUSE_UUID"),
            os.getenv("USER"),
            os.getenv("PASSWORD"),
            1
        )
        deppy.dot("octopus.dot")

        result = await deppy.execute()
        result.dot("result.dot")
        with open("result.json", "w") as f:
            f.write(json.dumps(result.dump(), indent=4))


if __name__ == "__main__":
    asyncio.run(main())
