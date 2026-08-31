import os
import getpass
import requests
import yaml
from pathlib import Path


def get_env_or_prompt(name, secret=False):
    value = os.environ.get(name)
    if not value:
        prompt = f"{name}: "
        if secret:
            value = getpass.getpass(prompt)
        else:
            value = input(prompt)
    return value


def main():
    host = get_env_or_prompt("DATABRICKS_HOST").rstrip("/")
    token = get_env_or_prompt("DATABRICKS_TOKEN", secret=True)
    access_connector_id = get_env_or_prompt("ACCESS_CONNECTOR_ID")

    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml nao encontrado em {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    storage = config["azure"]["storage_account"]
    container = config["azure"]["container"]
    external_url = f"abfss://{container}@{storage}.dfs.core.windows.net/"

    headers = {"Authorization": f"Bearer {token}"}

    # Criar storage credential (system-assigned managed identity)
    cred_name = "marathon-storage-credential"
    resp = requests.post(
        f"{host}/api/2.1/unity-catalog/storage-credentials",
        headers=headers,
        json={
            "name": cred_name,
            "azure_managed_identity": {
                "access_connector_id": access_connector_id
            },
            "comment": "Credencial para acesso ao ADLS do case marathon"
        }
    )
    if resp.status_code == 200:
        print(f"Storage credential '{cred_name}' criada.")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print(f"Storage credential '{cred_name}' ja existia.")
    else:
        raise Exception(f"Erro ao criar storage credential: {resp.status_code} - {resp.text}")

    # Criar external location
    loc_name = "marathon-external-location"
    resp = requests.post(
        f"{host}/api/2.1/unity-catalog/external-locations",
        headers=headers,
        json={
            "name": loc_name,
            "url": external_url,
            "credential_name": cred_name,
            "comment": "External location para o data lake do case marathon"
        }
    )
    if resp.status_code == 200:
        print(f"External location '{loc_name}' criada: {external_url}")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print(f"External location '{loc_name}' ja existia.")
    else:
        raise Exception(f"Erro ao criar external location: {resp.status_code} - {resp.text}")

    # Criar catalog dedicado do projeto
    catalog_name = "marathon"
    storage_root = f"{external_url}catalogs/{catalog_name}/"
    resp = requests.post(
        f"{host}/api/2.1/unity-catalog/catalogs",
        headers=headers,
        json={
            "name": catalog_name,
            "storage_root": storage_root,
            "comment": "Catalog do case marathon"
        }
    )
    if resp.status_code == 200:
        print(f"Catalog '{catalog_name}' criado.")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print(f"Catalog '{catalog_name}' ja existia.")
    else:
        raise Exception(f"Erro ao criar catalog: {resp.status_code} - {resp.text}")

    # Salvar nome do catalog como segredo para os notebooks
    resp = requests.post(
        f"{host}/api/2.0/secrets/put",
        headers=headers,
        json={
            "scope": "marathon-scope",
            "key": "catalog_name",
            "string_value": catalog_name
        }
    )
    resp.raise_for_status()
    print("Segredo 'catalog_name' salvo no scope 'marathon-scope'.")

    print("Unity Catalog configurado. Os notebooks usam o catalog 'marathon'.")


if __name__ == "__main__":
    main()
