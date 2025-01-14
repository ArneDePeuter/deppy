from deppy.helpers.wrappers.dkr import Dkr, JsonDk, StringDk
from deppy.helpers.asyncclient import AsyncClient

from powerbi_auth import PowerBIAuth


class PowerBIApi(AsyncClient):
    def __init__(
            self,
            base_url: str,
            scope: str,
            access_token_url: str,
            client_id: str,
            client_secret: str,
    ):
        super().__init__(
            base_url=base_url,
            auth=PowerBIAuth(
                access_token_url=access_token_url,
                client_id=client_id,
                client_secret=client_secret,
                access_token_request_data={"scope": scope},
            )
        )

        self.workspaces_request = Dkr(
            url="/groups",
        )(self.get, "workspaces")

        self.dashboards_request = Dkr(
            url=StringDk("/groups/{workspace_id}/dashboards"),
        )(self.get, "dashboards")

        self.datasets_request = Dkr(
            url=StringDk("/groups/{workspace_id}/datasets"),
        )(self.get, "datasets")

        self.refreshes_request = Dkr(
            url=StringDk("/groups/{workspace_id}/datasets/{dataset_id}/refreshes"),
            params=JsonDk({"top": "{refreshes_top}"})
        )(self.get, "refreshes")

        self.refresh_schedule_request = Dkr(
            url=StringDk("/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"),
        )(self.get, "refresh_schedule")

        self.reports_request = Dkr(
            url=StringDk("/groups/{workspace_id}/reports"),
        )(self.get, "reports")

        self.pages_request = self.ignore_on_status_codes(
            Dkr(
                url=StringDk("/groups/{workspace_id}/reports/{report_id}/pages"),
            )(self.get, "pages"),
            [401, 404]
        )

        self.datasources_request = self.ignore_on_status_codes(
            Dkr(
                url=StringDk("/groups/{workspace_id}/reports/{report_id}/datasources"),
            )(self.get, "datasources"),
            [401, 404]
        )

        self.capacities_request = Dkr(
            url="/capacities",
        )(self.get, "capacities")

        self.workloads_request = Dkr(
            url=StringDk("/capacities/{capacity_id}/workloads"),
        )(self.get, "workloads")
