#!/usr/bin/env python3
"""
Habilita file events para a external location do Databricks.
Atribui as roles necessarias ao system-assigned managed identity
 do Azure Access Connector.

PRE-REQUISITO: registrar o provider EventGrid antes de rodar este script:
  az provider register --namespace Microsoft.EventGrid --subscription <SUBSCRIPTION_ID>
  az provider show --namespace Microsoft.EventGrid --query registrationState -o tsv
  (aguarda retornar "Registered")
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"


def run_command(cmd, check=True):
    print(f"  > {' '.join(cmd)}")
    result = subprocess.run(cmd, text=True, capture_output=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Comando falhou: {result.stderr or result.stdout}")
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Habilitar File Events para Databricks")
    parser.add_argument("--resource-group", help="Resource Group")
    parser.add_argument("--storage-account", help="Storage Account")
    parser.add_argument("--access-connector", help="Access Connector Name")
    parser.add_argument("--subscription", help="Subscription ID")
    args = parser.parse_args()

    if CONFIG_FILE.exists():
        config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
    else:
        config = {}

    rg = args.resource_group or config.get("azure", {}).get("resource_group", "rg-marathon-case")
    storage = args.storage_account or config.get("azure", {}).get("storage_account", "stmarathoncase")
    access_connector = args.access_connector or "ac-marathon-case-v2"

    subscription = args.subscription or run_command(
        ["az", "account", "show", "--query", "id", "-o", "tsv"]
    )

    scope_storage = f"/subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{storage}"
    scope_rg = f"/subscriptions/{subscription}/resourceGroups/{rg}"
    connector_id = f"/subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.Databricks/accessConnectors/{access_connector}"

    print(f"Obtendo principalId do Access Connector {access_connector} ...")
    principal_id = run_command(
        ["az", "resource", "show", "--ids", connector_id, "--query", "properties.managedIdentity.principalId", "-o", "tsv"]
    )
    print(f"principalId = {principal_id}")

    storage_roles = [
        "Storage Blob Data Contributor",
        "Storage Queue Data Contributor",
        "Storage Account Contributor",
    ]
    for role in storage_roles:
        print(f"Atribuindo '{role}' no storage account...")
        run_command([
            "az", "role", "assignment", "create",
            "--assignee-object-id", principal_id,
            "--assignee-principal-type", "ServicePrincipal",
            "--role", role,
            "--scope", scope_storage,
        ], check=False)

    print("Atribuindo 'EventGrid EventSubscription Contributor' no resource group...")
    run_command([
        "az", "role", "assignment", "create",
        "--assignee-object-id", principal_id,
        "--assignee-principal-type", "ServicePrincipal",
        "--role", "EventGrid EventSubscription Contributor",
        "--scope", scope_rg,
    ], check=False)

    print("Roles atribuidas. Pode levar alguns minutos para propagar no Azure.")


if __name__ == "__main__":
    main()
