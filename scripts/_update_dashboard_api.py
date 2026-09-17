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
DASH_FILE = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"
SETUP_STATE = ROOT / ".setup_state.json"

# Gold tables in the dashboard that need catalog prefix
GOLD_TABLES = [
    "kpi_summary",
    "finishers_by_year",
    "top_countries",
    "age_gender_profile",
    "weather_impact",
    "times_distribution",
    "marathon_comparison",
]

# Monitoring table (different schema)
MONITORING_TABLES = [
    "monitoring.data_quality_log",
]


def add_catalog_prefix(query: str, catalog: str) -> str:
    """Replace bare Gold table names with catalog.gold.tablename."""
    result = query
    for table in GOLD_TABLES:
        # Match the bare table name (not already prefixed with a catalog/schema)
        # Negative lookbehind for dot to avoid double-prefixing
        pattern = r'(?<![.\w])' + re.escape(table) + r'(?![.\w])'
        replacement = f"{catalog}.gold.{table}"
        result = re.sub(pattern, replacement, result)
    # Fix monitoring schema to be fully qualified
    result = result.replace("monitoring.data_quality_log", f"{catalog}.monitoring.data_quality_log")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="marathon",
                        help="Unity Catalog name (default: marathon)")
    parser.add_argument("--setup-state", default=str(SETUP_STATE),
                        help="Path to .setup_state.json")
    args = parser.parse_args()

    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        print("ERROR: DATABRICKS_TOKEN env var not set", file=sys.stderr)
        sys.exit(1)

    state = json.loads(Path(args.setup_state).read_text(encoding="utf-8"))
    host = "https://" + state["outputs"]["databricks_workspace_url"]
    dashboard_id = state["outputs"]["dashboard_id"]
    warehouse_id = state["outputs"]["sql_warehouse_id"]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Read dashboard JSON and inject catalog prefix into all queryLines
    dash = json.loads(DASH_FILE.read_text(encoding="utf-8"))
    for dataset in dash.get("datasets", []):
        dataset["queryLines"] = [
            add_catalog_prefix(q, args.catalog)
            for q in dataset.get("queryLines", [])
        ]

    serialized = json.dumps(dash, ensure_ascii=False)

    # Verify prefixing worked
    if f"{args.catalog}.gold.kpi_summary" not in serialized:
        print(f"WARNING: catalog prefix for '{args.catalog}' not found in serialized dashboard")

    print(f"Updating dashboard {dashboard_id} on {host} [catalog={args.catalog}]...")
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

    # Republish
    print("Republishing dashboard...")
    resp2 = requests.post(
        f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        headers=headers,
        json={"warehouse_id": warehouse_id, "embed_credentials": True},
        timeout=30,
    )
    if resp2.status_code not in (200, 204):
        print(f"Publish failed {resp2.status_code}: {resp2.text[:500]}")
        sys.exit(1)
    print("Dashboard republished successfully.")
    print(f"URL: {host}/dashboardsv3/{dashboard_id}/published")


if __name__ == "__main__":
    main()
