import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from databricks_auth import get_databricks_headers, get_databricks_token


PROJECT_ROOT = Path(__file__).parent.parent
ENVIRONMENTS = {
    "local": {
        "resource_group": "rg-marathon-case",
        "workspace": "dbw-marathon-case",
        "catalog": "marathon",
    },
    "prod": {
        "resource_group": "rg-marathon-prod",
        "workspace": "dbw-marathon-prod",
        "catalog": "marathon_prod",
    },
}
PROTECTED_RESOURCE_GROUP = "rg-marathon-bootstrap"


def run(command, check=True):
    if command[0] == "az":
        command[0] = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
        if not command[0]:
            raise RuntimeError("Azure CLI nao encontrado")
    result = subprocess.run(command, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return result.stdout.strip()


def workspace_info(config):
    exists = run(["az", "group", "exists", "--name", config["resource_group"], "-o", "tsv"])
    if exists.lower() != "true":
        return None
    raw = run([
        "az", "databricks", "workspace", "show",
        "--resource-group", config["resource_group"],
        "--name", config["workspace"],
        "-o", "json",
    ])
    data = json.loads(raw)
    return {
        "host": f"https://{data['workspaceUrl']}",
        "resource_id": data["id"],
    }


def api(method, host, headers, path, **kwargs):
    response = requests.request(method, f"{host}{path}", headers=headers, timeout=60, **kwargs)
    return response


def require_deleted(response, resource, allow_permission_error=False):
    if response.status_code in (200, 204, 404):
        print(f"[OK] {resource} removido ou inexistente")
        return
    if allow_permission_error and response.status_code == 403:
        print(f"[SKIP] {resource}: sem permissao (403) — sera orphan apos exclusao do RG")
        return
    raise RuntimeError(f"Falha ao excluir {resource}: {response.status_code} - {response.text}")


def stop_and_delete_jobs(host, headers):
    response = api("GET", host, headers, "/api/2.1/jobs/list", params={"name": "marathon-case-bronze-silver-gold", "limit": 100})
    response.raise_for_status()
    for job in response.json().get("jobs", []):
        job_id = job["job_id"]
        runs = api("GET", host, headers, "/api/2.1/jobs/runs/list", params={"job_id": job_id, "active_only": "true", "limit": 25})
        runs.raise_for_status()
        for active_run in runs.json().get("runs", []):
            cancel = api("POST", host, headers, "/api/2.1/jobs/runs/cancel", json={"run_id": active_run["run_id"]})
            if cancel.status_code not in (200, 400, 404):
                cancel.raise_for_status()
        require_deleted(api("POST", host, headers, "/api/2.1/jobs/delete", json={"job_id": job_id}), f"workflow {job_id}")


def clean_unity_catalog(host, headers, catalog):
    prefix = catalog.replace("_", "-")
    require_deleted(
        api("DELETE", host, headers, f"/api/2.1/unity-catalog/catalogs/{catalog}", params={"force": "true"}),
        f"catalogo {catalog}",
    )
    for suffix in ("", "-v2"):
        location = f"{prefix}-external-location{suffix}"
        require_deleted(
            api("DELETE", host, headers, f"/api/2.1/unity-catalog/external-locations/{location}", params={"force": "true"}),
            f"external location {location}",
            allow_permission_error=True,
        )
    for suffix in ("", "-v2"):
        credential = f"{prefix}-storage-credential{suffix}"
        require_deleted(
            api("DELETE", host, headers, f"/api/2.1/unity-catalog/storage-credentials/{credential}", params={"force": "true"}),
            f"storage credential {credential}",
            allow_permission_error=True,
        )


def reset_local_files():
    state_file = PROJECT_ROOT / ".setup_state.json"
    state_file.write_text(json.dumps({"completed": [], "outputs": {}}, indent=2), encoding="utf-8")
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        dynamic = {
            "DATABRICKS_HOST",
            "DATABRICKS_WORKSPACE_ID",
            "DATABRICKS_AZURE_RESOURCE_ID",
            "ACCESS_CONNECTOR_ID",
            "DATABRICKS_HTTP_PATH",
        }
        lines = env_file.read_text(encoding="utf-8").splitlines()
        lines = [line for line in lines if line.split("=", 1)[0] not in dynamic]
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("[OK] Estado local resetado")


def main():
    parser = argparse.ArgumentParser(description="Destroi um ambiente Marathon sem deixar objetos no Unity Catalog")
    parser.add_argument("--environment", choices=ENVIRONMENTS, required=True)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    config = ENVIRONMENTS[args.environment]
    resource_group = config["resource_group"]
    if resource_group == PROTECTED_RESOURCE_GROUP or "bootstrap" in resource_group.lower():
        raise RuntimeError("O resource group de bootstrap nunca pode ser destruido por este script")
    if not args.yes:
        confirmation = input(f"Digite {resource_group} para confirmar a destruicao: ").strip()
        if confirmation != resource_group:
            print("Destruicao cancelada")
            return

    info = workspace_info(config)
    if info:
        os.environ["DATABRICKS_HOST"] = info["host"]
        os.environ["DATABRICKS_AZURE_RESOURCE_ID"] = info["resource_id"]
        headers = get_databricks_headers(get_databricks_token(use_environment=False))
        stop_and_delete_jobs(info["host"], headers)
        clean_unity_catalog(info["host"], headers, config["catalog"])
    else:
        print("[WARN] Workspace inexistente; nao foi possivel limpar objetos do Unity Catalog")

    run(["az", "group", "delete", "--name", resource_group, "--yes", "--no-wait"])
    print(f"[OK] Destruicao de {resource_group} iniciada")
    while run(["az", "group", "exists", "--name", resource_group, "-o", "tsv"]).lower() == "true":
        print(f"Aguardando exclusao de {resource_group}...")
        time.sleep(20)
    if args.environment == "local":
        reset_local_files()
    print(f"[OK] Ambiente {args.environment} destruido; {PROTECTED_RESOURCE_GROUP} preservado")


if __name__ == "__main__":
    main()
