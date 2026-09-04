#!/usr/bin/env python3
"""
Configura o Unity Catalog para o projeto marathon.

Esse script eh chamado pelo setup_all.py apos a criacao do workspace Databricks.
Ele pode:
- Criar um novo metastore na conta Databricks
- Atribuir o workspace a um metastore existente
- Criar storage credential, external location e catalog

Requer:
- DATABRICKS_ACCOUNT_ID
- DATABRICKS_HOST
- DATABRICKS_TOKEN (PAT ou Azure AD token)
- access_connector_id (do Terraform output)
- storage_account e container (do config.yaml)
"""

import json
import os
import sys
import time
from pathlib import Path

import requests
import yaml


PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"


def databricks_api(method, host, token, path, json_data=None, params=None, timeout=30):
    url = f"{host.rstrip('/')}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if method == "GET":
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    elif method == "POST":
        resp = requests.post(url, headers=headers, json=json_data, timeout=timeout)
    elif method == "PUT":
        resp = requests.put(url, headers=headers, json=json_data, timeout=timeout)
    elif method == "PATCH":
        resp = requests.patch(url, headers=headers, json=json_data, timeout=timeout)
    elif method == "DELETE":
        resp = requests.delete(url, headers=headers, timeout=timeout)
    else:
        raise ValueError(f"Metodo HTTP nao suportado: {method}")
    return resp


def account_api(method, account_id, token, path, json_data=None, params=None):
    host = "https://accounts.azuredatabricks.net"
    return databricks_api(method, host, token, f"/api/2.1/accounts/{account_id}{path}", json_data, params)


def workspace_api(method, host, token, path, json_data=None, params=None):
    return databricks_api(method, host, token, path, json_data, params)


def resource_exists(url, token, name, items_key):
    resp = databricks_api("GET", url, token, url)
    if resp.status_code != 200:
        return False
    data = resp.json()
    items = data.get(items_key, [])
    return any(item.get("name") == name for item in items)


def list_metastores(account_id, token):
    resp = account_api("GET", account_id, token, "/metastores")
    if resp.status_code != 200:
        print(f"Falha ao listar metastores: {resp.status_code} - {resp.text}")
        return []
    return resp.json().get("metastores", [])


def create_metastore(account_id, token, name, storage_root, region):
    resp = account_api("POST", account_id, token, "/metastores", {
        "name": name,
        "storage_root": storage_root,
        "region": region,
    })
    if resp.status_code == 200:
        return resp.json()["metastore_id"]
    if "already exists" in resp.text.lower() or resp.status_code == 409:
        # Tenta encontrar o ID do existente
        for ms in list_metastores(account_id, token):
            if ms.get("name") == name:
                return ms["metastore_id"]
    raise RuntimeError(f"Erro ao criar metastore: {resp.status_code} - {resp.text}")


def assign_workspace_to_metastore(account_id, token, workspace_id, metastore_id):
    # Account API GA
    resp = account_api(
        "PUT",
        account_id,
        token,
        f"/workspaces/{workspace_id}/metastores/{metastore_id}",
        {"default_catalog_name": "hive_metastore"},
    )
    if resp.status_code in (200, 201, 204):
        return True
    # Fallback para workspace-level API
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    resp2 = workspace_api(
        "PUT",
        host,
        token,
        f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore",
        {"metastore_id": metastore_id, "default_catalog_name": "hive_metastore"},
    )
    if resp2.status_code in (200, 201, 204):
        return True
    raise RuntimeError(f"Erro ao atribuir metastore: {resp.status_code} - {resp.text} | {resp2.status_code} - {resp2.text}")


def create_storage_credential(host, token, name, access_connector_id):
    if resource_exists(host, token, name, "storage_credentials"):
        print(f"Storage credential '{name}' ja existe")
        return
    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/storage-credentials", {
        "name": name,
        "azure_managed_identity": {"access_connector_id": access_connector_id},
        "comment": "Credencial para acesso ao ADLS do case marathon",
    })
    if resp.status_code == 200 or "already exists" in resp.text.lower() or resp.status_code == 409:
        print(f"Storage credential '{name}' criada/verificada")
    else:
        raise RuntimeError(f"Erro ao criar storage credential: {resp.status_code} - {resp.text}")


def create_external_location(host, token, name, url, credential_name):
    if resource_exists(host, token, name, "external_locations"):
        print(f"External location '{name}' ja existe")
        return
    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/external-locations", {
        "name": name,
        "url": url,
        "credential_name": credential_name,
        "comment": "External location para o data lake do case marathon",
    })
    if resp.status_code == 200 or "already exists" in resp.text.lower() or resp.status_code == 409:
        print(f"External location '{name}' criada/verificada")
    else:
        raise RuntimeError(f"Erro ao criar external location: {resp.status_code} - {resp.text}")


def create_catalog(host, token, name, storage_root):
    if resource_exists(host, token, name, "catalogs"):
        print(f"Catalog '{name}' ja existe")
        return
    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/catalogs", {
        "name": name,
        "storage_root": storage_root,
        "comment": "Catalog do case marathon",
    })
    if resp.status_code == 200 or "already exists" in resp.text.lower() or resp.status_code == 409:
        print(f"Catalog '{name}' criado/verificado")
    else:
        raise RuntimeError(f"Erro ao criar catalog: {resp.status_code} - {resp.text}")


def main():
    account_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    workspace_id = os.environ.get("DATABRICKS_WORKSPACE_ID")
    access_connector_id = os.environ.get("ACCESS_CONNECTOR_ID")

    if not all([host, token, workspace_id, access_connector_id]):
        print("Variaveis obrigatorias: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WORKSPACE_ID, ACCESS_CONNECTOR_ID")
        sys.exit(1)

    workspace_id = int(workspace_id)

    config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
    storage = config["azure"]["storage_account"]
    container = config["azure"]["container"]
    region = config["azure"].get("location", "eastus")
    external_url = f"abfss://{container}@{storage}.dfs.core.windows.net/"

    # Verifica se workspace ja tem metastore atribuido
    print(f"Verificando se Unity Catalog esta ativado no workspace {workspace_id}...")
    resp = workspace_api("GET", host, token, "/api/2.1/unity-catalog/catalogs")
    print(f"Resposta: {resp.status_code} - {resp.text[:200]}")
    if resp.status_code == 200:
        catalogs = resp.json().get("catalogs", [])
        catalog_names = [c.get("name") for c in catalogs]
        print(f"Unity Catalog ativado. Catalogs: {catalog_names}")
    else:
        print(f"Unity Catalog NAO ativado (status {resp.status_code}).")
        if not account_id:
            print("Requer DATABRICKS_ACCOUNT_ID para criar/atribuir metastore.")
            print("Abra o workspace, clique no icone do usuario > Manage Account/Account Console e copie o account_id da URL.")
            sys.exit(1)

        print("Criando/escolhendo metastore...")
        metastores = list_metastores(account_id, token)
        if metastores:
            print(f"Metastores existentes na regiao {region}:")
            for ms in metastores:
                print(f"  - {ms.get('name')} ({ms.get('region')}): {ms.get('metastore_id')}")
            same_region = [ms for ms in metastores if ms.get("region") == region]
            if same_region:
                metastore_id = same_region[0]["metastore_id"]
                print(f"Usando metastore existente: {metastore_id}")
            else:
                print(f"Nenhum metastore na regiao {region}. Criando novo...")
                metastore_id = create_metastore(account_id, token, "marathon-metastore", f"{external_url}metastore", region)
        else:
            print("Nenhum metastore encontrado. Criando novo...")
            metastore_id = create_metastore(account_id, token, "marathon-metastore", f"{external_url}metastore", region)

        print(f"Atribuindo metastore {metastore_id} ao workspace {workspace_id}...")
        assign_workspace_to_metastore(account_id, token, workspace_id, metastore_id)
        print("Aguardando propagacao do metastore (60s)...")
        time.sleep(60)

    print("Criando recursos do Unity Catalog...")
    create_storage_credential(host, token, "marathon-storage-credential", access_connector_id)
    create_external_location(host, token, "marathon-external-location", external_url, "marathon-storage-credential")
    create_catalog(host, token, "marathon", f"{external_url}catalogs/marathon/")

    print("Unity Catalog configurado com sucesso.")


if __name__ == "__main__":
    main()
