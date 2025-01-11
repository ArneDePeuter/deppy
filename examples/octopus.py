from deppy import Deppy, IgnoreResult, Scope
from deppy.wrappers import Dkr, JsonDk, StringDk

import httpx
import asyncio
import os
import json


async def octopus(
        base_url: str,
        software_house_uuid: str,
        user: str,
        password: str,
        locale_id: int = 1
) -> Scope:
    async with httpx.AsyncClient(base_url=base_url) as client:
        async def request(**kwargs):
            response = await client.request(**kwargs)
            if response.status_code != 200:
                return IgnoreResult(reason=f"Request failed with status code {response.status_code}", data=response)
            return response.json()

        deppy = Deppy()

        auth_node = deppy.node(
            Dkr(
                method="POST",
                url="/authentication",
                headers=JsonDk({"softwareHouseUuid": "{software_house_uuid}"}),
                json=JsonDk({"user": "{user}", "password": "{password}"})
            )(request, "auth"),
            secret=True
        ).software_house_uuid(
            deppy.secret(software_house_uuid, name="software house uuid")
        ).user(
            deppy.secret(user, name="user")
        ).password(
            deppy.secret(password, name="password")
        )

        dossiers_node = deppy.node(
            Dkr(
                method="GET",
                url="/dossiers",
                headers=JsonDk({"Token": "{token}"}),
            )(request, "dossiers")
        ).token(auth_node, extractor=lambda x: x["token"])

        dossier_id_node = deppy.node(
            lambda dossier: dossier["dossierKey"]["id"],
            name="dossier_id"
        ).dossier(dossiers_node, loop=True)

        dossiers_token = deppy.node(
            Dkr(
                method="POST",
                url="/dossiers",
                headers=JsonDk({"Token": "{token}"}),
                params=JsonDk({"dossierId": "{dossier_id}", "localeId": "{locale_id}"}),
            )(request, "dossiers_token"),
            secret=True
        ).token(
            auth_node, extractor=lambda x: x["token"]
        ).dossier_id(
            dossier_id_node
        ).locale_id(
            deppy.const(locale_id, name="locale id")
        )

        bookyears_node = deppy.node(
            Dkr(
                method="GET",
                url=StringDk("/dossiers/{dossier_id}/bookyears"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
            )(request, "bookyears")
        ).dossier_token(
            dossiers_token, extractor=lambda token_info: token_info["Dossiertoken"]
        ).dossier_id(dossier_id_node)

        deppy.dot("octopus.dot")

        return await deppy.execute()


async def main():
    result = await octopus(
        base_url="https://service.inaras.be/octopus-rest-api/v1",
        software_house_uuid=str(os.environ["SOFTWARE_HOUSE_UUID"]),
        user=str(os.environ["USER"]),
        password=str(os.environ["PASSWORD"])
    )
    print(json.dumps(result.dump()))

asyncio.run(main())
