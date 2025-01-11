from deppy import Deppy, IgnoreResult
from deppy.wrappers import Dkr, JsonDk, StringDk

import httpx
import asyncio
import os
import json


def octopus(
        request,
        software_house_uuid: str,
        user: str,
        password: str,
        locale_id: int = 1
) -> Deppy:
    return Deppy.from_dict(
        {
            "constants": {
                "locale_id": locale_id
            },
            "secrets": {
                "software_house_uuid": software_house_uuid,
                "user": user,
                "password": password
            },
            "nodes": {
                "auth": {
                    "func": Dkr(
                        method="POST",
                        url="/authentication",
                        headers=JsonDk({"softwareHouseUuid": "{software_house_uuid}"}),
                        json=JsonDk({"user": "{user}", "password": "{password}"})
                    )(request),
                    "secret": True,
                    "dependencies": [
                        {"node": "software_house_uuid", "name": "software_house_uuid"},
                        {"node": "user", "name": "user"},
                        {"node": "password", "name": "password"}
                    ]
                },
                "token": {
                    "func": lambda auth: auth["token"],
                    "secret": True,
                    "dependencies": [
                        {"node": "auth", "name": "auth"}
                    ]
                },
                "dossiers": {
                    "func": Dkr(
                        method="GET",
                        url="/dossiers",
                        headers=JsonDk({"Token": "{token}"}),
                    )(request),
                    "dependencies": [
                        {"node": "token"}
                    ]
                },
                "dossier_id": {
                    "func": lambda dossier: dossier["dossierKey"]["id"],
                    "dependencies": [
                        {"node": "dossiers", "name": "dossier", "loop": True}
                    ]
                },
                "get_dossier_token": {
                    "func": Dkr(
                        method="POST",
                        url="/dossiers",
                        headers=JsonDk({"Token": "{token}"}),
                        params=JsonDk({"dossierId": "{dossier_id}", "localeId": "{locale_id}"}),
                    )(request),
                    "secret": True,
                    "dependencies": [
                        {"node": "token"},
                        {"node": "dossier_id"},
                        {"node": "locale_id"}
                    ]
                },
                "dossier_token": {
                    "func": lambda token_info: token_info["Dossiertoken"],
                    "secret": True,
                    "dependencies": [
                        {"node": "get_dossier_token", "name": "token_info"}
                    ]
                },
                "bookyears": {
                    "func": Dkr(
                        method="GET",
                        url=StringDk("/dossiers/{dossier_id}/bookyears"),
                        headers=JsonDk({"dossierToken": "{dossier_token}"}),
                    )(request, "bookyears"),
                    "dependencies": [
                        {"node": "dossier_token"},
                        {"node": "dossier_id"}
                    ]
                },
                "bookyear_id": {
                    "func": lambda bookyear: bookyear["bookyearKey"]["id"],
                    "dependencies": [
                        {"node": "bookyears", "name": "bookyear", "loop": True}
                    ]
                },
            }
        }
    )


async def main():
    base_url = "https://service.inaras.be/octopus-rest-api/v1"
    async with httpx.AsyncClient(base_url=base_url) as client:
        async def request(**kwargs):
            response = await client.request(**kwargs)
            if response.status_code != 200:
                return IgnoreResult(reason=f"Request failed with status code {response.status_code}", data=response)
            return response.json()

        octo = octopus(
            request=request,
            software_house_uuid=str(os.environ["SOFTWARE_HOUSE_UUID"]),
            user=str(os.environ["USER"]),
            password=str(os.environ["PASSWORD"])
        )
        octo.dot("octopus.dot")
        result = await octo.execute()
        print(json.dumps(result.dump()))
        result.dot("result.dot")
        print(json.dumps(result(octo.get_node_by_name("bookyears"))))

asyncio.run(main())
