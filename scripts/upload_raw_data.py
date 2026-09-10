import argparse
import time
from pathlib import Path

import yaml
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def wait_for_storage_access(container_client, attempts=6):
    for attempt in range(1, attempts + 1):
        try:
            container_client.get_container_properties()
            return
        except HttpResponseError as exc:
            if exc.status_code not in (401, 403) or attempt == attempts:
                raise
            delay = min(5 * 2 ** (attempt - 1), 60)
            print(f"RBAC ainda nao propagado (tentativa {attempt}/{attempts}). Nova tentativa em {delay}s...")
            time.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description="Upload raw CSVs to ADLS raw layer")
    parser.add_argument("--local-dir", default="data/raw", help="Local raw data directory")
    args = parser.parse_args()

    local_dir = Path(args.local_dir)
    if not local_dir.exists():
        raise FileNotFoundError(f"Diretorio nao encontrado: {local_dir}")

    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml nao encontrado em {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    storage = config["azure"]["storage_account"]
    container = config["azure"]["container"]
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage}.blob.core.windows.net",
        credential=credential,
    )
    container_client = blob_service_client.get_container_client(container)
    wait_for_storage_access(container_client)

    csv_files = sorted(f for f in local_dir.iterdir() if f.is_file() and f.suffix.lower() == ".csv")
    if not csv_files:
        print(f"Nenhum CSV encontrado em {local_dir}")
        return

    for f in csv_files:
        blob_name = f"raw/{f.name}"
        blob_client = container_client.get_blob_client(blob_name)
        with open(f, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Upload: {f} -> abfss://{container}@{storage}.dfs.core.windows.net/{blob_name}")

    print("Upload concluido com identidade Microsoft Entra ID.")


if __name__ == "__main__":
    main()
