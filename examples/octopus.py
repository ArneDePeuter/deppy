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
        await asyncio.sleep(1)
        result = await function(*args, **kwargs)
        result.raise_for_status()
        return result.json()

    return wrapper


def ignore_on_status_codes(function: Callable[P, Awaitable[httpx.Response]], status_codes: Iterable[int]) -> Callable[P, Union[IgnoreResult, Any]]:
    @wraps(function)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Union[IgnoreResult, Any]:
        try:
            result = await function(*args, **kwargs)
            return result
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
        stated_kwargs: StatedKwargs
) -> Deppy:
    client.request = request_wrapper(client.request)

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

    accounts_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/accounts"),
        headers=JsonDk({"dossierToken": "{dossier_token}"}),
        params=JsonDk({"bookyearId": "{bookyear_id}"})
    )(client.get, "accounts")

    products_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/products"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "products")

    vatcodes_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/vatcodes"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "vatcodes")

    relations_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/relations"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "relations")

    product_groups_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/productgroups"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "product_groups")

    bookyears_request = Dkr(
        url=StringDk("/dossiers/{dossier_id}/bookyears"),
        headers=JsonDk({"dossierToken": "{dossier_token}"})
    )(client.get, "bookyears")

    def modified_request(request):
        return ignore_on_status_codes(
            stated_kwargs(
                request,
                name="modified_timestamp",
                initial_value=initial_modified_timestamp,
                produce_function=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                keys=["dossier_id"],
            ),
            {404}
        )

    modified_accounts_request = modified_request(
        Dkr(
            url=StringDk("/dossiers/{dossier_id}/accounts/modified"),
            headers=JsonDk({"dossierToken": "{dossier_token}"}),
            params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
        )(client.get, "modified_accounts")
    )

    modified_bookings_request = modified_request(
        Dkr(
            url=StringDk("/dossiers/{dossier_id}/bookyears/-1/bookings/modified"),
            headers=JsonDk({"dossierToken": "{dossier_token}"}),
            params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}", "journalTypeId": "-1"})
        )(client.get, "modified_bookings")
    )

    modified_relations_request = modified_request(
        Dkr(
            url=StringDk("/dossiers/{dossier_id}/relations/modified"),
            headers=JsonDk({"dossierToken": "{dossier_token}"}),
            params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
        )(client.get, "modified_relations")
    )

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
    bookyear_id_node = deppy.add_output(bookyears_node, "bookyear_id", lambda bookyear: bookyear["bookyearKey"]["id"], loop=True)

    accounts_node = deppy.add_node(accounts_request)
    products_node = deppy.add_node(products_request)
    vatcodes_node = deppy.add_node(vatcodes_request)
    relations_node = deppy.add_node(relations_request)
    product_groups_node = deppy.add_node(product_groups_request)
    modified_accounts_node = deppy.add_node(modified_accounts_request)
    modified_bookings_node = deppy.add_node(modified_bookings_request)
    modified_relations_node = deppy.add_node(modified_relations_request)

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
    deppy.add_edge(dossier_token_node, accounts_node, "dossier_token")
    deppy.add_edge(dossier_id_node, accounts_node, "dossier_id")
    deppy.add_edge(bookyear_id_node, accounts_node, "bookyear_id")
    deppy.add_edge(dossier_token_node, products_node, "dossier_token")
    deppy.add_edge(dossier_id_node, products_node, "dossier_id")
    deppy.add_edge(dossier_token_node, vatcodes_node, "dossier_token")
    deppy.add_edge(dossier_id_node, vatcodes_node, "dossier_id")
    deppy.add_edge(dossier_token_node, relations_node, "dossier_token")
    deppy.add_edge(dossier_id_node, relations_node, "dossier_id")
    deppy.add_edge(dossier_token_node, product_groups_node, "dossier_token")
    deppy.add_edge(dossier_id_node, product_groups_node, "dossier_id")
    deppy.add_edge(dossier_token_node, modified_accounts_node, "dossier_token")
    deppy.add_edge(dossier_id_node, modified_accounts_node, "dossier_id")
    deppy.add_edge(dossier_token_node, modified_bookings_node, "dossier_token")
    deppy.add_edge(dossier_id_node, modified_bookings_node, "dossier_id")
    deppy.add_edge(dossier_token_node, modified_relations_node, "dossier_token")
    deppy.add_edge(dossier_id_node, modified_relations_node, "dossier_id")

    return deppy


async def main():
    async with httpx.AsyncClient(base_url="https://service.inaras.be/octopus-rest-api/v1") as client:
        stated_kwargs = StatedKwargs()
        deppy = octopus(
            client,
            os.getenv("SOFTWARE_HOUSE_UUID"),
            os.getenv("USER"),
            os.getenv("PASSWORD"),
            1,
            "2000-01-01 00:00:00.000",
            stated_kwargs
        )
        deppy.dot("octopus.dot")

        with stated_kwargs:
            result = await deppy.execute()

        result.dot("result.dot")
        with open("result.json", "w") as f:
            f.write(json.dumps(result.dump(), indent=4, default=str))


if __name__ == "__main__":
    asyncio.run(main())
