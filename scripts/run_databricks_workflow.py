import os
import time

import requests
from dotenv import load_dotenv

from databricks_auth import get_databricks_headers, get_databricks_token


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def main():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    headers = get_databricks_headers(get_databricks_token())
    job_name = "marathon-case-bronze-silver-gold"
    response = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers=headers,
        params={"name": job_name, "limit": 1},
        timeout=30,
    )
    response.raise_for_status()
    jobs = response.json().get("jobs", [])
    if not jobs:
        raise RuntimeError(f"Workflow nao encontrado: {job_name}")

    job_id = jobs[0]["job_id"]
    response = requests.get(
        f"{host}/api/2.1/jobs/runs/list",
        headers=headers,
        params={"job_id": job_id, "active_only": "true", "limit": 1},
        timeout=30,
    )
    response.raise_for_status()
    active_runs = response.json().get("runs", [])
    if active_runs:
        run_id = active_runs[0]["run_id"]
        print(f"Workflow ja esta em execucao: job_id={job_id}, run_id={run_id}")
    else:
        response = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            headers=headers,
            json={"job_id": job_id},
            timeout=30,
        )
        response.raise_for_status()
        run_id = response.json()["run_id"]
        print(f"Workflow iniciado: job_id={job_id}, run_id={run_id}")

    unavailable_attempts = 0
    while True:
        response = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=30,
        )
        if response.status_code == 400 and unavailable_attempts < 12:
            unavailable_attempts += 1
            print(f"Workflow {run_id} ainda nao propagado. Aguardando...")
            time.sleep(10)
            continue
        response.raise_for_status()
        unavailable_attempts = 0
        state = response.json()["state"]
        lifecycle = state["life_cycle_state"]
        result = state.get("result_state")
        print(f"Workflow {run_id}: {lifecycle}{f' / {result}' if result else ''}")
        if lifecycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            if result != "SUCCESS":
                raise RuntimeError(f"Workflow terminou com status {result or lifecycle}")
            return
        time.sleep(30)


if __name__ == "__main__":
    main()
