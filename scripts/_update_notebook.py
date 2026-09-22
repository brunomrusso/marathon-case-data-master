"""Atualiza um notebook no Databricks Workspace via API."""
import base64
import json
import os
import sys

import requests
import yaml
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG = ROOT / "config" / "config.yaml"


def main():
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        print("ERRO: defina DATABRICKS_TOKEN")
        sys.exit(1)

    state_path = ROOT / ".setup_state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        url = state.get("outputs", {}).get("databricks_workspace_url")
        if url:
            host = f"https://{url}"
        else:
            config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
            host = f"https://{config['azure']['databricks_workspace']}"
    else:
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        host = f"https://{config['azure']['databricks_workspace']}"
    notebook_file = sys.argv[1] if len(sys.argv) > 1 else "notebooks/00_bronze_orchestrator.py"
    workspace_path = f"/Shared/marathon-case/{notebook_file}"
    if workspace_path.endswith(".py"):
        workspace_path = workspace_path[:-3]

    content = (ROOT / notebook_file).read_text(encoding="utf-8")
    b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")

    resp = requests.post(
        f"{host}/api/2.0/workspace/import",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "path": workspace_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": b64,
        },
    )
    print(resp.status_code, resp.text[:200])
    resp.raise_for_status()
    print(f"Notebook {workspace_path} atualizado.")


if __name__ == "__main__":
    main()
