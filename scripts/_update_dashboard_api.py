#!/usr/bin/env python3
"""Update the Databricks AI/BI dashboard via API and republish it."""
import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).parent.parent
DASH_FILE = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"
SETUP_STATE = ROOT / ".setup_state.json"

token = os.environ.get("DATABRICKS_TOKEN", "")
if not token:
    print("ERROR: DATABRICKS_TOKEN env var not set", file=sys.stderr)
    sys.exit(1)

state = json.loads(SETUP_STATE.read_text(encoding="utf-8"))
host = "https://" + state["outputs"]["databricks_workspace_url"]
dashboard_id = state["outputs"]["dashboard_id"]

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Read updated dashboard JSON
dash = json.loads(DASH_FILE.read_text(encoding="utf-8"))
serialized = json.dumps(dash, ensure_ascii=False)

print(f"Updating dashboard {dashboard_id} on {host}...")
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
    json={"warehouse_id": state["outputs"]["sql_warehouse_id"], "embed_credentials": True},
    timeout=30,
)
if resp2.status_code not in (200, 204):
    print(f"Publish failed {resp2.status_code}: {resp2.text[:500]}")
    sys.exit(1)
print("Dashboard republished successfully.")
print(f"URL: {host}/dashboardsv3/{dashboard_id}/published")
