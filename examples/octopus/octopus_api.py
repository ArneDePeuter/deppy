from deppy.helpers.wrappers.dkr import Dkr, JsonDk, StringDk
from deppy.helpers.wrappers.stated_kwargs import StatedKwargs
from deppy.helpers.asyncclient import AsyncClient
from datetime import datetime


class OctopusApi(AsyncClient):
    def __init__(self, base_url: str, initial_modified_timestamp: str, state_file: str):
        self.stated_kwargs = StatedKwargs(state_file=state_file)
        self.initial_modified_timestamp = initial_modified_timestamp
        super().__init__(base_url=base_url)

        self.auth_request = Dkr(
            url="/authentication",
            headers=JsonDk({"softwareHouseUuid": "{software_house_uuid}"}),
            json=JsonDk({"user": "{user}", "password": "{password}"}),
        )(self.post, "auth")

        self.dossiers_request = Dkr(
            url="/dossiers",
            headers=JsonDk({"Token": "{token}"})
        )(self.get, "dossiers")

        self.dossier_token_info_request = Dkr(
            url="/dossiers",
            headers=JsonDk({"Token": "{token}"}),
            params=JsonDk({"dossierId": "{dossier_id}", "localeId": "{locale_id}"})
        )(self.post, "dossier_token_info")

        self.accounts_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/accounts"),
            headers=JsonDk({"dossierToken": "{dossier_token}"}),
            params=JsonDk({"bookyearId": "{bookyear_id}"})
        )(self.get, "accounts")

        self.products_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/products"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(self.get, "products")

        self.vatcodes_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/vatcodes"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(self.get, "vatcodes")

        self.relations_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/relations"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(self.get, "relations")

        self.product_groups_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/productgroups"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(self.get, "product_groups")

        self.bookyears_request = Dkr(
            url=StringDk("/dossiers/{dossier_id}/bookyears"),
            headers=JsonDk({"dossierToken": "{dossier_token}"})
        )(self.get, "bookyears")

        self.modified_accounts_request = self.create_modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/accounts/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
            )(self.get, "modified_accounts")
        )

        self.modified_bookings_request = self.create_modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/bookyears/-1/bookings/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}", "journalTypeId": "-1"})
            )(self.get, "modified_bookings")
        )

        self.modified_relations_request = self.create_modified_request(
            Dkr(
                url=StringDk("/dossiers/{dossier_id}/relations/modified"),
                headers=JsonDk({"dossierToken": "{dossier_token}"}),
                params=JsonDk({"modifiedTimeStamp": "{modified_timestamp}"})
            )(self.get, "modified_relations")
        )

    def create_modified_request(self, request):
        return self.ignore_on_status_codes(
            self.stated_kwargs(
                request,
                name="modified_timestamp",
                initial_value=self.initial_modified_timestamp,
                produce_function=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                keys=["dossier_id"],
            ),
            {404}
        )

    async def __aenter__(self):
        await super().__aenter__()
        self.stated_kwargs.__enter__()
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        self.stated_kwargs.__exit__(exc_type, exc_val, exc_tb)
        return None
