import argparse
import base64
import os
import requests
from pathlib import Path


CHUNK_SIZE = 1024 * 1024  # 1 MB
DBFS_RAW_PATH = "dbfs:/FileStore/marathon/raw"


def get_creds():
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        raise ValueError("Defina DATABRICKS_HOST e DATABRICKS_TOKEN")
    return host.rstrip("/"), token


def dbfs_mkdir(host, token, path):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(f"{host}/api/2.0/dbfs/mkdirs", headers=headers, json={"path": path})
    if resp.status_code not in (200, 201):
        raise Exception(f"mkdir failed: {resp.text}")


def dbfs_upload(host, token, local_path, dbfs_path):
    headers = {"Authorization": f"Bearer {token}"}
    # Cria (ou sobrescreve) o arquivo no DBFS
    resp = requests.post(f"{host}/api/2.0/dbfs/create", headers=headers, json={
        "path": dbfs_path,
        "overwrite": True
    })
    resp.raise_for_status()
    handle = resp.json()["handle"]

    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            b64 = base64.b64encode(chunk).decode("ascii")
            resp = requests.post(f"{host}/api/2.0/dbfs/add-block", headers=headers, json={
                "handle": handle,
                "data": b64
            })
            resp.raise_for_status()

    resp = requests.post(f"{host}/api/2.0/dbfs/close", headers=headers, json={"handle": handle})
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser(description="Upload raw CSVs to Databricks DBFS")
    parser.add_argument("--local-dir", default="data/raw", help="Local raw data directory")
    parser.add_argument("--dbfs-dir", default=DBFS_RAW_PATH, help="DBFS destination path")
    args = parser.parse_args()

    host, token = get_creds()
    local_dir = Path(args.local_dir)
    if not local_dir.exists():
        raise FileNotFoundError(f"Diretorio nao encontrado: {local_dir}")

    dbfs_mkdir(host, token, args.dbfs_dir)

    csv_files = sorted(f for f in local_dir.iterdir() if f.is_file() and f.suffix.lower() == ".csv")
    if not csv_files:
        print(f"Nenhum CSV encontrado em {local_dir}")
        return

    for f in csv_files:
        dest = f"{args.dbfs_dir}/{f.name}"
        print(f"Upload: {f} -> {dest}")
        dbfs_upload(host, token, f, dest)

    print("Upload concluido.")


if __name__ == "__main__":
    main()
