from typing import Callable, TypeVar, ParamSpec, Union, Any, Awaitable, Iterable
from functools import wraps
import httpx
from deppy import IgnoreResult
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
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code in status_codes:
                return IgnoreResult()
            raise

    return wrapper


class OctopusApi:
    def __init__(
            self,
            client: httpx.AsyncClient,
            initial_modified_timestamp: str,
            stated_kwargs: StatedKwargs
    ):
        client.request = request_wrapper(client.request)

        self.auth_request = Dkr(
            url="/authentication",
            headers=JsonDk({"softwareHouseUuid": "{software_house_uuid}"}),
            json=JsonDk({"user": "{user}", "password": "{password}"}),
        )(client.post, "auth")

        self.dossiers_request = Dkr(
            url="/dossiers",
            headers=JsonDk({"Token": "{token}"})
        )(client.get, "dossiers")

        self.dossier_token_info_request = Dkr(
            url="/dossiers",
            headers=JsonDk({"Token": "{token}"}),
            params=JsonDk({"dossierId": "{dossier_id}", "localeId": "{locale_id}"})
        )(client.post, "dossier_token_info")

        self.accounts_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/accounts"),
            headers=JsonDk({"dossierToken": "{dossier_token}"}),
            params=JsonDk({"bookyearId": "{bookyear_id}"})
        )(client.get, "accounts")

        self.products_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/products"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(client.get, "products")

        self.vatcodes_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/vatcodes"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(client.get, "vatcodes")

        self.relations_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/relations"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(client.get, "relations")

        self.product_groups_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/productgroups"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(client.get, "product_groups")

        self.bookyears_request = Dkr(
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

        self.modified_accounts_request = modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/accounts/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
            )(client.get, "modified_accounts")
        )

        self.modified_bookings_request = modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/bookyears/-1/bookings/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}", "journalTypeId": "-1"})
            )(client.get, "modified_bookings")
        )

        self.modified_relations_request = modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/relations/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
            )(client.get, "modified_relations")
        )
