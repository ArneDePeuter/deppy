from deppy import Deppy

from octopus_api import OctopusApi


class Octopus(Deppy):
    def __init__(self, initial_modified_timestamp: str = "2000-01-01 00:00:00.000", state_file: str = "state.json"):
        super().__init__("octopus")

        self.api = OctopusApi(initial_modified_timestamp=initial_modified_timestamp, state_file=state_file)

        self.software_house_uuid_node = self.add_secret(name="software_house_uuid")
        self.user_node = self.add_secret(name="user")
        self.password_node = self.add_secret(name="password")
        self.locale_id_node = self.add_const(name="locale_id")

        self.auth_node = self.add_node(self.api.auth_request, secret=True)
        self.token_node = self.add_output(self.auth_node, "token", lambda data: data["token"])

        self.dossiers_node = self.add_node(self.api.dossiers_request)
        self.dossier_id_node = self.add_output(self.dossiers_node, "dossier_id", lambda dossier: dossier["dossierKey"]["id"], loop=True)

        self.dossier_token_info_node = self.add_node(self.api.dossier_token_info_request, secret=True)
        self.dossier_token_node = self.add_output(self.dossier_token_info_node, "dossier_token", lambda data: data["Dossiertoken"])

        self.bookyears_node = self.add_node(self.api.bookyears_request)
        self.bookyear_id_node = self.add_output(self.bookyears_node, "bookyear_id", lambda bookyear: bookyear["bookyearKey"]["id"], loop=True)

        self.accounts_node = self.add_node(self.api.accounts_request)
        self.products_node = self.add_node(self.api.products_request)
        self.vatcodes_node = self.add_node(self.api.vatcodes_request)
        self.relations_node = self.add_node(self.api.relations_request)
        self.product_groups_node = self.add_node(self.api.product_groups_request)
        self.modified_accounts_node = self.add_node(self.api.modified_accounts_request)
        self.modified_bookings_node = self.add_node(self.api.modified_bookings_request)
        self.modified_relations_node = self.add_node(self.api.modified_relations_request)

        # create all edges
        self.add_edge(self.software_house_uuid_node, self.auth_node, "software_house_uuid")
        self.add_edge(self.user_node, self.auth_node, "user")
        self.add_edge(self.password_node, self.auth_node, "password")
        self.add_edge(self.token_node, self.dossiers_node, "token")
        self.add_edge(self.dossier_id_node, self.dossier_token_info_node, "dossier_id")
        self.add_edge(self.token_node, self.dossier_token_info_node, "token")
        self.add_edge(self.locale_id_node, self.dossier_token_info_node, "locale_id")
        self.add_edge(self.dossier_token_node, self.bookyears_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.bookyears_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.accounts_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.accounts_node, "dossier_id")
        self.add_edge(self.bookyear_id_node, self.accounts_node, "bookyear_id")
        self.add_edge(self.dossier_token_node, self.products_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.products_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.vatcodes_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.vatcodes_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.relations_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.relations_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.product_groups_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.product_groups_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.modified_accounts_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.modified_accounts_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.modified_bookings_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.modified_bookings_node, "dossier_id")
        self.add_edge(self.dossier_token_node, self.modified_relations_node, "dossier_token")
        self.add_edge(self.dossier_id_node, self.modified_relations_node, "dossier_id")

    def configure(self, base_url, **kwargs) -> Deppy:
        self.api.base_url = base_url
        return super().configure(**kwargs)

    async def __aenter__(self):
        await self.api.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api.__aexit__(exc_type, exc_val, exc_tb)
