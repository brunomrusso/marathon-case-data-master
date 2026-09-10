import os

from azure.identity import DefaultAzureCredential


DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
AZURE_MANAGEMENT_SCOPE = "https://management.core.windows.net/.default"


def get_databricks_token(use_environment=True):
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if use_environment and token:
        return token
    return DefaultAzureCredential().get_token(DATABRICKS_SCOPE).token


def get_databricks_headers(token=None):
    headers = {"Authorization": f"Bearer {token or get_databricks_token()}"}
    resource_id = os.environ.get("DATABRICKS_AZURE_RESOURCE_ID", "").strip()
    if resource_id:
        management_token = DefaultAzureCredential().get_token(AZURE_MANAGEMENT_SCOPE).token
        headers["X-Databricks-Azure-SP-Management-Token"] = management_token
        headers["X-Databricks-Azure-Workspace-Resource-Id"] = resource_id
    return headers
