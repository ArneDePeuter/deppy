from deppy import Deppy

from octopus_api import OctopusApi


class Octopus(Deppy):
    def __init__(
            self,
            base_url: str,
            initial_modified_timestamp: str,
            state_file: str,
            software_house_uuid: str,
            user: str,
            password: str,
            locale_id: int
    ):
        self.api = OctopusApi(base_url=base_url, initial_modified_timestamp=initial_modified_timestamp, state_file=state_file)
        super().__init__()

        software_house_uuid_node = self.add_secret(software_house_uuid, name="software_house_uuid")
        user_node = self.add_secret(user, name="user")
        password_node = self.add_secret(password, name="password")
        locale_id_node = self.add_const(locale_id, name="locale_id")

        # create all nodes
        auth_node = self.add_node(self.api.auth_request, secret=True)
        token_node = self.add_output(auth_node, "token", lambda data: data["token"])

        dossiers_node = self.add_node(self.api.dossiers_request)
        dossier_id_node = self.add_output(dossiers_node, "dossier_id", lambda dossier: dossier["dossierKey"]["id"], loop=True)

        dossier_token_info_node = self.add_node(self.api.dossier_token_info_request, secret=True)
        dossier_token_node = self.add_output(dossier_token_info_node, "dossier_token", lambda data: data["Dossiertoken"])

        bookyears_node = self.add_node(self.api.bookyears_request)
        bookyear_id_node = self.add_output(bookyears_node, "bookyear_id", lambda bookyear: bookyear["bookyearKey"]["id"], loop=True)

        accounts_node = self.add_node(self.api.accounts_request)
        products_node = self.add_node(self.api.products_request)
        vatcodes_node = self.add_node(self.api.vatcodes_request)
        relations_node = self.add_node(self.api.relations_request)
        product_groups_node = self.add_node(self.api.product_groups_request)
        modified_accounts_node = self.add_node(self.api.modified_accounts_request)
        modified_bookings_node = self.add_node(self.api.modified_bookings_request)
        modified_relations_node = self.add_node(self.api.modified_relations_request)

        # create all edges
        self.add_edge(software_house_uuid_node, auth_node, "software_house_uuid")
        self.add_edge(user_node, auth_node, "user")
        self.add_edge(password_node, auth_node, "password")
        self.add_edge(token_node, dossiers_node, "token")
        self.add_edge(dossier_id_node, dossier_token_info_node, "dossier_id")
        self.add_edge(token_node, dossier_token_info_node, "token")
        self.add_edge(locale_id_node, dossier_token_info_node, "locale_id")
        self.add_edge(dossier_token_node, bookyears_node, "dossier_token")
        self.add_edge(dossier_id_node, bookyears_node, "dossier_id")
        self.add_edge(dossier_token_node, accounts_node, "dossier_token")
        self.add_edge(dossier_id_node, accounts_node, "dossier_id")
        self.add_edge(bookyear_id_node, accounts_node, "bookyear_id")
        self.add_edge(dossier_token_node, products_node, "dossier_token")
        self.add_edge(dossier_id_node, products_node, "dossier_id")
        self.add_edge(dossier_token_node, vatcodes_node, "dossier_token")
        self.add_edge(dossier_id_node, vatcodes_node, "dossier_id")
        self.add_edge(dossier_token_node, relations_node, "dossier_token")
        self.add_edge(dossier_id_node, relations_node, "dossier_id")
        self.add_edge(dossier_token_node, product_groups_node, "dossier_token")
        self.add_edge(dossier_id_node, product_groups_node, "dossier_id")
        self.add_edge(dossier_token_node, modified_accounts_node, "dossier_token")
        self.add_edge(dossier_id_node, modified_accounts_node, "dossier_id")
        self.add_edge(dossier_token_node, modified_bookings_node, "dossier_token")
        self.add_edge(dossier_id_node, modified_bookings_node, "dossier_id")
        self.add_edge(dossier_token_node, modified_relations_node, "dossier_token")
        self.add_edge(dossier_id_node, modified_relations_node, "dossier_id")

    async def __aenter__(self):
        await self.api.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api.__aexit__(exc_type, exc_val, exc_tb)