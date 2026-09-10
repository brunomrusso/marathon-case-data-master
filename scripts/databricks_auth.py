import os

from azure.identity import DefaultAzureCredential


DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"


def get_databricks_token(use_environment=True):
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if use_environment and token:
        return token
    return DefaultAzureCredential().get_token(DATABRICKS_SCOPE).token
