from deppy.blueprint import Blueprint, Node, Const, Object, Output

from powerbi_api import PowerBIApi


class PowerBI(Blueprint):
    api = Object(PowerBIApi)

    refreshes_top = Const()

    workspaces = Node(api.workspaces_request)
    workspace_id = Output(workspaces, lambda workspace: workspace["id"], loop=True)

    dashboards = Node(api.dashboards_request)
    datasets = Node(api.datasets_request)
    refreshable_datasets = Output(
        datasets,
        lambda datasets: [dataset for dataset in datasets if dataset["isRefreshable"]],
    )
    refreshable_dataset_id = Output(
        refreshable_datasets, lambda dataset: dataset["id"], loop=True
    )

    refreshes = Node(api.refreshes_request)
    refresh_schedule = Node(api.refresh_schedule_request)

    reports = Node(api.reports_request)
    powerbi_reports = Output(
        reports,
        lambda reports: [
            report for report in reports if report["reportType"] == "PowerBIReport"
        ],
    )
    powerbi_report_id = Output(powerbi_reports, lambda report: report["id"], loop=True)
    paginated_reports = Output(
        reports,
        lambda reports: [
            report for report in reports if report["reportType"] == "PaginatedReport"
        ],
    )
    paginated_report_id = Output(
        paginated_reports, lambda report: report["id"], loop=True
    )

    pages = Node(api.pages_request)
    datasources = Node(api.datasources_request)

    capacities = Node(api.capacities_request)
    capacity_id = Output(capacities, lambda capacity: capacity["id"], loop=True)

    workloads = Node(api.workloads_request)

    edges = [
        (workspace_id, dashboards, "workspace_id"),
        (workspace_id, datasets, "workspace_id"),
        (workspace_id, refreshes, "workspace_id"),
        (refreshable_dataset_id, refreshes, "dataset_id"),
        (refreshes_top, refreshes, "refreshes_top"),
        (workspace_id, refresh_schedule, "workspace_id"),
        (refreshable_dataset_id, refresh_schedule, "dataset_id"),
        (workspace_id, reports, "workspace_id"),
        (workspace_id, pages, "workspace_id"),
        (powerbi_report_id, pages, "report_id"),
        (workspace_id, datasources, "workspace_id"),
        (paginated_report_id, datasources, "report_id"),
        (capacity_id, workloads, "capacity_id"),
    ]
