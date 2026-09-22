import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

from databricks_auth import get_databricks_headers, get_databricks_token


PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def main():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = get_databricks_headers(get_databricks_token())

    state_file = PROJECT_ROOT / ".setup_state.json"
    if not state_file.exists():
        print("INFO: .setup_state.json nao encontrado; nada para parar.")
        sys.exit(0)

    state = json.loads(state_file.read_text(encoding="utf-8"))
    warehouse_id = state.get("outputs", {}).get("sql_warehouse_id")
    if not warehouse_id:
        print("INFO: sql_warehouse_id nao encontrado no setup_state; nada para parar.")
        sys.exit(0)

    print(f"Parando SQL warehouse {warehouse_id}...")
    resp = requests.post(
        f"{host}/api/2.0/sql/warehouses/{warehouse_id}/stop",
        headers=headers,
        timeout=60,
    )
    if resp.status_code in (200, 202, 204):
        print("SQL warehouse parado com sucesso.")
    elif resp.status_code == 400 and "warehouse is not running" in resp.text.lower():
        print("SQL warehouse ja estava parado.")
    else:
        print(f"WARN: nao foi possivel parar o warehouse ({resp.status_code}): {resp.text[:200]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
