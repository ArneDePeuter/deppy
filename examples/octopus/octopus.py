from octopus_api import OctopusApi

from deppy.deppy_blueprint import Const, Secret, Node, DeppyBluePrint, Output, Object


class Octopus(DeppyBluePrint):
    api = Object(OctopusApi)

    software_house_uuid = Secret()
    user = Secret()
    password = Secret()
    locale_id = Const()

    auth = Node(api.auth_request, secret=True)
    token = Output(auth, lambda data: data["token"])

    dossiers = Node(api.dossiers_request)
    dossier_id = Output(dossiers, lambda dossier: dossier["dossierKey"]["id"], loop=True)

    dossier_token_info = Node(api.dossier_token_info_request, secret=True)
    dossier_token = Output(dossier_token_info, lambda data: data["Dossiertoken"])

    bookyears = Node(api.bookyears_request)
    bookyear_id = Output(bookyears, lambda bookyear: bookyear["bookyearKey"]["id"], loop=True)

    accounts = Node(api.accounts_request)
    products = Node(api.products_request)
    vatcodes = Node(api.vatcodes_request)
    relations = Node(api.relations_request)
    product_groups = Node(api.product_groups_request)
    modified_accounts = Node(api.modified_accounts_request)
    modified_bookings = Node(api.modified_bookings_request)
    modified_relations = Node(api.modified_relations_request)

    edges = [
        (software_house_uuid, auth, "software_house_uuid"),
        (user, auth, "user"),
        (password, auth, "password"),
        (token, dossiers, "token"),
        (dossier_id, dossier_token_info, "dossier_id"),
        (token, dossier_token_info, "token"),
        (locale_id, dossier_token_info, "locale_id"),
        (dossier_token, bookyears, "dossier_token"),
        (dossier_id, bookyears, "dossier_id"),
        (dossier_token, accounts, "dossier_token"),
        (dossier_id, accounts, "dossier_id"),
        (bookyear_id, accounts, "bookyear_id"),
        (dossier_token, products, "dossier_token"),
        (dossier_id, products, "dossier_id"),
        (dossier_token, vatcodes, "dossier_token"),
        (dossier_id, vatcodes, "dossier_id"),
        (dossier_token, relations, "dossier_token"),
        (dossier_id, relations, "dossier_id"),
        (dossier_token, product_groups, "dossier_token"),
        (dossier_id, product_groups, "dossier_id"),
        (dossier_token, modified_accounts, "dossier_token"),
        (dossier_id, modified_accounts, "dossier_id"),
        (dossier_token, modified_bookings, "dossier_token"),
        (dossier_id, modified_bookings, "dossier_id"),
        (dossier_token, modified_relations, "dossier_token"),
        (dossier_id, modified_relations, "dossier_id")
    ]
