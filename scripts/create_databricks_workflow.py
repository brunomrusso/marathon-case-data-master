import argparse
import os
import requests
import yaml
from pathlib import Path


def get_env_or_raise(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Variável de ambiente {name} não definida")
    return value


def main():
    parser = argparse.ArgumentParser(description="Create Databricks Workflow")
    parser.add_argument("--repo-path", help="Workspace path to repo (e.g. /Workspace/Repos/user/marathon-case-data-master)")
    parser.add_argument("--cluster-id", help="Existing cluster ID (optional)")
    args = parser.parse_args()

    host = get_env_or_raise("DATABRICKS_HOST").rstrip("/")
    token = get_env_or_raise("DATABRICKS_TOKEN")
    repo_path = args.repo_path or get_env_or_raise("DATABRICKS_REPO_PATH")
    cluster_id = args.cluster_id or os.environ.get("DATABRICKS_EXISTING_CLUSTER_ID")

    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    storage = config["azure"]["storage_account"]
    container = config["azure"]["container"]
    raw_url = f"abfss://{container}@{storage}.dfs.core.windows.net/raw/"

    tasks = [
        {
            "task_key": "bronze_orchestrator",
            "notebook_task": {"notebook_path": f"{repo_path}/notebooks/00_bronze_orchestrator"},
            "description": "Ingest all raw CSVs into Bronze",
        },
        {
            "task_key": "silver_etl",
            "depends_on": [{"task_key": "bronze_orchestrator"}],
            "notebook_task": {"notebook_path": f"{repo_path}/notebooks/02_silver_etl"},
            "description": "Run Silver ETL",
        },
        {
            "task_key": "gold_aggregations",
            "depends_on": [{"task_key": "silver_etl"}],
            "notebook_task": {"notebook_path": f"{repo_path}/notebooks/03_gold_aggregations"},
            "description": "Generate Gold tables",
        },
    ]

    # Trigger: file_arrival (requer EventGrid registrado) ou schedule (mais simples)
    trigger_type = os.environ.get("TRIGGER_TYPE", "file_arrival")
    if trigger_type == "schedule":
        trigger = {
            "pause_status": "UNPAUSED",
            "schedule": {
                "quartz_cron_expression": "0 0/15 * * * ?",
                "timezone_id": "America/Sao_Paulo",
                "pause_status": "UNPAUSED",
            },
        }
    else:
        trigger = {
            "pause_status": "UNPAUSED",
            "file_arrival": {
                "url": raw_url,
                "min_time_between_triggers_seconds": 900,
                "wait_after_last_change_seconds": 60,
            },
        }

    if cluster_id:
        for task in tasks:
            task["existing_cluster_id"] = cluster_id
        job = {
            "name": "marathon-case-bronze-silver-gold",
            "trigger": trigger,
            "tasks": tasks,
        }
    else:
        job = {
            "name": "marathon-case-bronze-silver-gold",
            "trigger": trigger,
            "job_clusters": [
            {
                "job_cluster_key": "marathon_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2,
                    "auto_termination_minutes": 20,
                },
            }
            ],
            "tasks": [{**task, "job_cluster_key": "marathon_cluster"} for task in tasks],
        }

    resp = requests.post(
        f"{host}/api/2.1/jobs/create",
        headers={"Authorization": f"Bearer {token}"},
        json=job,
    )
    resp.raise_for_status()
    print(f"Workflow criado: job_id={resp.json()['job_id']}")


if __name__ == "__main__":
    main()
