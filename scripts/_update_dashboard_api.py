#!/usr/bin/env python3
"""
Update the Databricks AI/BI dashboard via API and republish it.

The source JSON uses short table names (kpi_summary, etc.).
This script prefixes them with {catalog}.gold. so the dashboard runs
in the correct Unity Catalog namespace without requiring Terraform's
dataset_catalog setting.

Usage:
  DATABRICKS_TOKEN=<token> python _update_dashboard_api.py [--catalog marathon]
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).parent.parent
DASH_DIR = ROOT / "dashboard" / "databricks"
MAIN_DASH_FILE = DASH_DIR / "marathon_dashboard.lvdash.json"
OBS_DASH_FILE = DASH_DIR / "observability_dashboard.lvdash.json"
SETUP_STATE = ROOT / ".setup_state.json"

# Gold tables in the main dashboard that need catalog prefix
GOLD_TABLES = [
    "kpi_summary",
    "finishers_by_year",
    "top_countries",
    "age_gender_profile",
    "weather_impact",
    "times_distribution",
    "marathon_comparison",
]


def add_prefixes(query: str, replacements: list[tuple[str, str]]) -> str:
    """Replace bare table names with fully-qualified catalog.schema.table names."""
    result = query
    for bare, qualified in replacements:
        pattern = r'(?<![.\w])' + re.escape(bare) + r'(?![.\w])'
        result = re.sub(pattern, qualified, result)
    return result


def patch_dashboard(host: str, token: str, dashboard_id: str, warehouse_id: str,
                    dash_file: Path, replacements: list[tuple[str, str]], label: str):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    dash = json.loads(dash_file.read_text(encoding="utf-8"))
    for dataset in dash.get("datasets", []):
        dataset["queryLines"] = [
            add_prefixes(q, replacements)
            for q in dataset.get("queryLines", [])
        ]

    serialized = json.dumps(dash, ensure_ascii=False)

    print(f"Updating {label} {dashboard_id} on {host}...")
    resp = requests.patch(
        f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}",
        headers=headers,
        json={"serialized_dashboard": serialized},
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"PATCH failed {resp.status_code}: {resp.text[:500]}")
        sys.exit(1)
    data = resp.json()
    print(f"Updated: {data.get('display_name')} [{data.get('lifecycle_state')}]")

    print(f"Republishing {label}...")
    resp2 = requests.post(
        f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        headers=headers,
        json={"warehouse_id": warehouse_id, "embed_credentials": True},
        timeout=30,
    )
    if resp2.status_code not in (200, 204):
        print(f"Publish failed {resp2.status_code}: {resp2.text[:500]}")
        sys.exit(1)
    print(f"{label} republished successfully.")
    print(f"URL: {host}/dashboardsv3/{dashboard_id}/published")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="marathon",
                        help="Unity Catalog name (default: marathon)")
    parser.add_argument("--setup-state", default=str(SETUP_STATE),
                        help="Path to .setup_state.json")
    parser.add_argument("--dashboard", choices=["main", "observability", "both"],
                        default="both",
                        help="Which dashboard to repatch (default: both)")
    args = parser.parse_args()

    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        print("ERROR: DATABRICKS_TOKEN env var not set", file=sys.stderr)
        sys.exit(1)

    state = json.loads(Path(args.setup_state).read_text(encoding="utf-8"))
    host = "https://" + state["outputs"]["databricks_workspace_url"]
    warehouse_id = state["outputs"]["sql_warehouse_id"]

    gold_replacements = [(tbl, f"{args.catalog}.gold.{tbl}") for tbl in GOLD_TABLES]
    monitoring_replacements = [("data_quality_log", f"{args.catalog}.monitoring.data_quality_log")]

    if args.dashboard in ("main", "both"):
        dashboard_id = state["outputs"]["dashboard_id"]
        patch_dashboard(host, token, dashboard_id, warehouse_id,
                        MAIN_DASH_FILE, gold_replacements, "dashboard principal")

    if args.dashboard in ("observability", "both"):
        obs_id = state["outputs"].get("observability_dashboard_id")
        if not obs_id:
            print("WARNING: observability_dashboard_id nao encontrado no setup_state; pulando.")
        else:
            patch_dashboard(host, token, obs_id, warehouse_id,
                            OBS_DASH_FILE, monitoring_replacements, "dashboard de observabilidade")


if __name__ == "__main__":
    main()
