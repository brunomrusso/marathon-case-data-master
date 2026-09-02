import argparse
import getpass
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
import yaml


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
    storage_key = get_env_or_prompt("STORAGE_ACCESS_KEY", secret=True)

    account_url = f"https://{storage}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_key)

    try:
        blob_service_client.create_container(container)
        print(f"Container '{container}' criado.")
    except Exception as e:
        if "ContainerAlreadyExists" not in str(e):
            raise

    csv_files = sorted(f for f in local_dir.iterdir() if f.is_file() and f.suffix.lower() == ".csv")
    if not csv_files:
        print(f"Nenhum CSV encontrado em {local_dir}")
        return

    for f in csv_files:
        blob_name = f"raw/{f.name}"
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
        with open(f, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Upload: {f} -> abfss://{container}@{storage}.dfs.core.windows.net/{blob_name}")

    print("Upload concluido.")


if __name__ == "__main__":
    main()
