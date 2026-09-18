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
- Sessao Microsoft Entra ID disponivel via DefaultAzureCredential
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
from dotenv import load_dotenv

from databricks_auth import get_databricks_headers, get_databricks_token


PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)


def databricks_api(method, host, token, path, json_data=None, params=None, timeout=30, retries=5):
    url = f"{host.rstrip('/')}{path}"
    headers = get_databricks_headers(token) if "accounts.azuredatabricks.net" not in host else {"Authorization": f"Bearer {token}"}
    headers["Content-Type"] = "application/json"
    for attempt in range(retries):
        if method == "GET":
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        elif method == "POST":
            resp = requests.post(url, headers=headers, json=json_data, timeout=timeout)
        elif method == "PUT":
            resp = requests.put(url, headers=headers, json=json_data, timeout=timeout)
        elif method == "PATCH":
            resp = requests.patch(url, headers=headers, json=json_data, timeout=timeout)
        elif method == "DELETE":
            resp = requests.delete(url, headers=headers, params=params, timeout=timeout)
        else:
            raise ValueError(f"Metodo HTTP nao suportado: {method}")
        if resp.status_code != 429:
            return resp
        wait = 2 ** attempt + 1
        print(f"Rate limit (429) em {path}. Aguardando {wait}s antes de tentar novamente...")
        time.sleep(wait)
    return resp


def account_api(method, account_id, token, path, json_data=None, params=None):
    host = "https://accounts.azuredatabricks.net"
    return databricks_api(method, host, token, f"/api/2.1/accounts/{account_id}{path}", json_data, params)


def workspace_api(method, host, token, path, json_data=None, params=None):
    return databricks_api(method, host, token, path, json_data, params)


def resource_exists(host, path, token, name, items_key):
    resp = databricks_api("GET", host, token, path)
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


def _try_recover_orphan(host, token, resource_type, resource_path, name):
    """Try to delete or transfer ownership of an orphaned UC resource (403)."""
    del_resp = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/{resource_path}/{name}", params={"force": "true"})
    if del_resp.status_code in (200, 204):
        print(f"  Credential/location orfã '{name}' deletada com sucesso")
        return True
    # Try ownership transfer
    whoami = workspace_api("GET", host, token, "/api/2.0/preview/scim/v2/Me")
    if whoami.status_code == 200:
        my_username = whoami.json().get("userName", "")
        patch_resp = workspace_api("PATCH", host, token, f"/api/2.1/unity-catalog/{resource_path}/{name}", {"owner": my_username})
        if patch_resp.status_code == 200:
            print(f"  Owner de '{name}' transferido para {my_username}")
            del2 = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/{resource_path}/{name}", params={"force": "true"})
            if del2.status_code in (200, 204):
                return True
    return False


def _validate_storage_credential(host, token, cred_name, access_connector_id):
    """Validate that an existing storage credential works by checking via API.
    Returns True if the credential appears valid (passes UC validation)."""
    try:
        resp = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/storage-credentials/{cred_name}")
        if resp.status_code != 200:
            return False
        # Ask UC to validate the credential
        val = workspace_api("POST", host, token, f"/api/2.1/unity-catalog/storage-credentials/{cred_name}/validate",
                            {"external_location_name": None})
        if val.status_code == 200:
            results = val.json().get("results", [])
            # Any operation that fails indicates a broken credential
            failed = [r for r in results if r.get("result") == "FAIL"]
            if failed:
                print(f"  Credencial '{cred_name}' falhou validacao: {failed[0].get('message','')}")
                return False
        return True
    except Exception:
        return True  # assume ok if validation API is unavailable


def create_storage_credential(host, token, name, external_location_name, access_connector_id):
    """Create or recover storage credential. Returns (recreated: bool, actual_name: str)."""
    print(f"Garantindo que a storage credential '{name}' esteja correta...")
    current = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/storage-credentials/{name}")
    if current.status_code == 200:
        current_connector = current.json().get("azure_managed_identity", {}).get("access_connector_id", "")
        if current_connector.lower() == access_connector_id.lower():
            # ARM resource ID matches, but the managed identity may have been recreated.
            # Validate the credential actually works before skipping recreation.
            if _validate_storage_credential(host, token, name, access_connector_id):
                print(f"Storage credential '{name}' ja aponta para o Access Connector atual e esta valida")
                return False, name
            print(f"Storage credential '{name}' aponta para o mesmo ARM ID mas falhou validacao — recriando...")
        del_ext = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/external-locations/{external_location_name}", params={"force": "true"})
        if del_ext.status_code not in (200, 204, 404):
            raise RuntimeError(f"Erro ao remover external location antiga: {del_ext.status_code} - {del_ext.text}")
        del_resp = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/storage-credentials/{name}", params={"force": "true"})
        if del_resp.status_code not in (200, 204):
            raise RuntimeError(f"Erro ao remover storage credential antiga: {del_resp.status_code} - {del_resp.text}")
        time.sleep(5)
    elif current.status_code == 403:
        print(f"Storage credential '{name}' existe mas pertence a outra identidade (403). Tentando recuperar...")
        workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/external-locations/{external_location_name}", params={"force": "true"})
        recovered = _try_recover_orphan(host, token, "storage-credentials", "storage-credentials", name)
        if not recovered:
            # Use an alternative name to avoid conflict with the orphaned resource
            alt_name = f"{name}-v2"
            alt_ext = f"{external_location_name}-v2"
            print(f"  Nao foi possivel recuperar '{name}'. Usando nome alternativo: '{alt_name}'")
            # Check if the alt name already exists and is correct
            alt_check = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/storage-credentials/{alt_name}")
            if alt_check.status_code == 200:
                alt_connector = alt_check.json().get("azure_managed_identity", {}).get("access_connector_id", "")
                if alt_connector.lower() == access_connector_id.lower():
                    print(f"Storage credential '{alt_name}' ja aponta para o Access Connector atual")
                    return False, alt_name
                workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/external-locations/{alt_ext}", params={"force": "true"})
                workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/storage-credentials/{alt_name}", params={"force": "true"})
                time.sleep(3)
            name = alt_name
        else:
            time.sleep(5)
    elif current.status_code != 404:
        raise RuntimeError(f"Erro ao consultar storage credential: {current.status_code} - {current.text}")

    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/storage-credentials", {
        "name": name,
        "azure_managed_identity": {"access_connector_id": access_connector_id},
        "comment": "Credencial para acesso ao ADLS do case marathon",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Erro ao criar storage credential: {resp.status_code} - {resp.text}")
    print(f"Storage credential '{name}' criada")
    return True, name


def _clear_overlapping_locations(host, token, target_url):
    """Remove or recover any external locations that overlap with target_url."""
    resp = workspace_api("GET", host, token, "/api/2.1/unity-catalog/external-locations")
    if resp.status_code != 200:
        return
    target = target_url.rstrip("/")
    for loc in resp.json().get("external_locations", []):
        loc_url = loc.get("url", "").rstrip("/")
        loc_name = loc.get("name", "")
        # Check overlap: one is prefix of the other
        if target.startswith(loc_url) or loc_url.startswith(target):
            print(f"  External location conflitante encontrada: '{loc_name}' -> {loc_url}")
            del_resp = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/external-locations/{loc_name}", params={"force": "true"})
            if del_resp.status_code in (200, 204):
                print(f"  Removida: '{loc_name}'")
            elif del_resp.status_code == 403:
                recovered = _try_recover_orphan(host, token, "external-locations", "external-locations", loc_name)
                if recovered:
                    print(f"  Recuperada e removida: '{loc_name}'")
                else:
                    print(f"  AVISO: nao foi possivel remover location orfã '{loc_name}'")


def create_external_location(host, token, name, url, credential_name):
    current = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/external-locations/{name}")
    if current.status_code == 200:
        data = current.json()
        if data.get("url", "").rstrip("/") == url.rstrip("/") and data.get("credential_name") == credential_name:
            print(f"External location '{name}' ja esta correta")
            return
        del_resp = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/external-locations/{name}", params={"force": "true"})
        if del_resp.status_code not in (200, 204):
            raise RuntimeError(f"Erro ao remover external location antiga: {del_resp.status_code} - {del_resp.text}")
    elif current.status_code == 403:
        print(f"External location '{name}' existe mas pertence a outra identidade (403). Tentando recuperar...")
        recovered = _try_recover_orphan(host, token, "external-locations", "external-locations", name)
        if not recovered:
            alt_name = f"{name}-v2"
            print(f"  Nao foi possivel recuperar '{name}'. Usando nome alternativo: '{alt_name}'")
            name = alt_name
        else:
            time.sleep(3)
    elif current.status_code != 404:
        raise RuntimeError(f"Erro ao consultar external location: {current.status_code} - {current.text}")

    # Clear any overlapping external locations before creating
    _clear_overlapping_locations(host, token, url)

    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/external-locations", {
        "name": name,
        "url": url,
        "credential_name": credential_name,
        "comment": "External location para o data lake do case marathon",
    })
    if resp.status_code == 200:
        print(f"External location '{name}' criada")
        return
    # If URL overlaps with an orphaned location we cannot remove, try a narrower sub-path
    if resp.status_code == 400 and "overlaps" in resp.text.lower():
        narrow_url = url.rstrip("/") + "/catalogs/"
        print(f"  URL conflita com location orfã. Tentando sub-path: {narrow_url}")
        resp2 = workspace_api("POST", host, token, "/api/2.1/unity-catalog/external-locations", {
            "name": name,
            "url": narrow_url,
            "credential_name": credential_name,
            "comment": "External location para o data lake do case marathon (sub-path)",
        })
        if resp2.status_code == 200:
            print(f"External location '{name}' criada em sub-path")
            return
        raise RuntimeError(f"Erro ao criar external location (sub-path): {resp2.status_code} - {resp2.text}")
    raise RuntimeError(f"Erro ao criar external location: {resp.status_code} - {resp.text}")


def grant_catalog_read_access(host, token, catalog_name):
    resp = workspace_api(
        "PATCH",
        host,
        token,
        f"/api/2.1/unity-catalog/permissions/catalog/{catalog_name}",
        {"changes": [{"principal": "account users", "add": ["BROWSE", "USE_CATALOG", "USE_SCHEMA", "SELECT"]}]},
    )
    if resp.status_code == 200:
        print("Acesso de leitura do catalogo concedido a account users")
    elif resp.status_code == 403:
        print(f"Sem permissao MANAGE no catalogo '{catalog_name}' (owner diferente). Grants existentes preservados.")
    else:
        raise RuntimeError(f"Erro ao conceder leitura no catalogo: {resp.status_code} - {resp.text}")


def create_catalog(host, token, name, storage_root, force_recreate=False):
    resp = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/catalogs/{name}")
    if resp.status_code == 200 and not force_recreate:
        print(f"Catalog '{name}' preservado")
        return
    if resp.status_code == 200:
        if force_recreate:
            print(f"Access Connector alterado. Recriando catalog '{name}'...")
            del_resp = workspace_api("DELETE", host, token, f"/api/2.1/unity-catalog/catalogs/{name}", params={"force": "true"})
            if del_resp.status_code in (200, 204):
                time.sleep(5)
            elif del_resp.status_code == 403:
                # Cannot delete orphaned catalog; try to take ownership
                recovered = _try_recover_orphan(host, token, "catalogs", "catalogs", name)
                if recovered:
                    time.sleep(5)
                else:
                    print(f"  Catalog '{name}' orfao nao pode ser removido. Reutilizando existente.")
                    return
            else:
                raise RuntimeError(f"Falha ao remover catalog: {del_resp.status_code} - {del_resp.text}")
        else:
            print(f"Catalog '{name}' preservado")
            return
    elif resp.status_code == 403:
        print(f"Catalog '{name}' existe mas pertence a outra identidade (403). Tentando recuperar...")
        recovered = _try_recover_orphan(host, token, "catalogs", "catalogs", name)
        if recovered:
            time.sleep(5)
        else:
            # Cannot recover; the catalog exists with data, reuse it
            print(f"  Catalog '{name}' orfao nao pode ser removido. Tentando reutilizar.")
            return
    elif resp.status_code != 404:
        raise RuntimeError(f"Erro ao consultar catalog: {resp.status_code} - {resp.text}")

    resp = workspace_api("POST", host, token, "/api/2.1/unity-catalog/catalogs", {
        "name": name,
        "storage_root": storage_root,
        "comment": "Catalog do case marathon",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Erro ao criar catalog: {resp.status_code} - {resp.text}")
    print(f"Catalog '{name}' criado")


def main():
    account_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = get_databricks_token()
    workspace_id = os.environ.get("DATABRICKS_WORKSPACE_ID")
    access_connector_id = os.environ.get("ACCESS_CONNECTOR_ID")
    catalog_name = os.environ.get("CATALOG_NAME", "marathon")

    if not host or not workspace_id or not access_connector_id:
        print("Variaveis obrigatorias: DATABRICKS_HOST, DATABRICKS_WORKSPACE_ID, ACCESS_CONNECTOR_ID")
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

    if not catalog_name:
        catalog_name = "marathon"
    print(f"Criando recursos do Unity Catalog (catalogo: {catalog_name})...")
    resource_prefix = catalog_name.replace("_", "-")
    credential_name = f"{resource_prefix}-storage-credential"
    external_location_name = f"{resource_prefix}-external-location"

    # Check if catalog already exists with correct storage root
    catalog_resp = workspace_api("GET", host, token, f"/api/2.1/unity-catalog/catalogs/{catalog_name}")
    catalog_storage_root = f"{external_url}catalogs/{catalog_name}/"
    catalog_exists_ok = (
        catalog_resp.status_code == 200
        and catalog_resp.json().get("storage_root", "").rstrip("/") == catalog_storage_root.rstrip("/")
    )

    credential_recreated, credential_name = create_storage_credential(host, token, credential_name, external_location_name, access_connector_id)
    # If credential name changed due to orphan recovery, update external location name too
    if credential_name.endswith("-v2") and not external_location_name.endswith("-v2"):
        external_location_name = f"{external_location_name}-v2"

    try:
        create_external_location(host, token, external_location_name, external_url, credential_name)
    except RuntimeError as e:
        if "overlaps" in str(e).lower() and catalog_exists_ok:
            print(f"  External location orfã bloqueia o URL mas catalog '{catalog_name}' ja existe e esta funcional. Pulando.")
        else:
            raise

    if catalog_exists_ok and not credential_recreated:
        print(f"Catalog '{catalog_name}' preservado (storage root correto)")
    else:
        create_catalog(host, token, catalog_name, catalog_storage_root, force_recreate=credential_recreated)
    grant_catalog_read_access(host, token, catalog_name)
    print(f"CATALOG_NAME={catalog_name}")

    print("Unity Catalog configurado com sucesso.")


if __name__ == "__main__":
    main()
