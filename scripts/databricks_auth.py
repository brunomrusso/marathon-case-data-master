import os
import shutil
import subprocess

from azure.identity import DefaultAzureCredential


DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
AZURE_MANAGEMENT_RESOURCE = "https://management.core.windows.net/"


def get_databricks_token(use_environment=True):
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if use_environment and token:
        return token
    return DefaultAzureCredential().get_token(DATABRICKS_SCOPE).token


def get_azure_management_token():
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if not az:
        raise RuntimeError("Azure CLI nao encontrado")
    result = subprocess.run(
        [az, "account", "get-access-token", "--resource", AZURE_MANAGEMENT_RESOURCE, "--query", "accessToken", "-o", "tsv"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def get_databricks_headers(token=None):
    headers = {"Authorization": f"Bearer {token or get_databricks_token()}"}
    resource_id = os.environ.get("DATABRICKS_AZURE_RESOURCE_ID", "").strip()
    if resource_id:
        headers["X-Databricks-Azure-SP-Management-Token"] = get_azure_management_token()
        headers["X-Databricks-Azure-Workspace-Resource-Id"] = resource_id
    return headers
