#!/usr/bin/env python3
"""
Fix KPI widgets on overview page:
- Switch total-finishers, total-editions, total-marathons back to 'summary'
  dataset so they respond to global marathon/year filters.
- Keep avg-time as table widget (only way to show HH:MM string) using
  'summary' dataset for filter responsiveness.
- Suppress column header in avg-time table to reduce visual clutter.
"""
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
DASH = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"

d = json.loads(DASH.read_text(encoding="utf-8"))

ov = next(p for p in d["pages"] if p["name"] == "overview")

# Expressions against 'summary' dataset (disaggregated=false → aggregates all rows)
# summary has: source, year, total_finishers, total_editions, total_marathons, avg_finish_time_min
COUNTER_CFG = {
    "total-finishers": {
        "field": "total_finishers",
        "expression": "SUM(total_finishers)",
        "displayName": "Finishers",
        "title": "Total de concluintes",
    },
    "total-editions": {
        "field": "total_editions",
        "expression": "COUNT(DISTINCT year)",
        "displayName": "Edicoes",
        "title": "Edicoes analisadas",
    },
    "total-marathons": {
        "field": "total_marathons",
        "expression": "COUNT(DISTINCT source)",
        "displayName": "Maratonas",
        "title": "Maratonas",
    },
}

# avg_finish_time_hm computed from avg_finish_time_min (already in summary)
# FLOOR(257 / 60) = 4h, MOD(257, 60) = 17m
AVG_TIME_EXPR = (
    "CONCAT("
    "CAST(FLOOR(AVG(avg_finish_time_min) / 60) AS INT), 'h ', "
    "LPAD(CAST(ROUND(MOD(AVG(avg_finish_time_min), 60), 0) AS INT), 2, '0'), 'm'"
    ")"
)

for wi in ov["layout"]:
    w = wi["widget"]
    name = w.get("name", "")

    if name in COUNTER_CFG:
        cfg = COUNTER_CFG[name]
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "summary",
                "fields": [{"name": cfg["field"], "expression": cfg["expression"]}],
                "disaggregated": False,
            },
        }]
        w["spec"]["encodings"]["value"] = {
            "fieldName": cfg["field"],
            "displayName": cfg["displayName"],
        }
        w["spec"]["frame"]["title"] = cfg["title"]
        w["spec"]["frame"]["showTitle"] = True
        # ensure no stale format spec
        w["spec"]["encodings"]["value"].pop("format", None)
        print(f"Counter restored: {name} -> summary.{cfg['field']}")

    elif name == "average-time":
        # Table widget: responds to filters via 'summary' dataset, no column header
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "summary",
                "fields": [{"name": "avg_finish_time_hm", "expression": AVG_TIME_EXPR}],
                "disaggregated": False,
            },
        }]
        w["spec"] = {
            "version": 2,
            "widgetType": "table",
            "encodings": {
                "columns": [{
                    "fieldName": "avg_finish_time_hm",
                    "displayName": "",        # suppress column header
                }]
            },
            "frame": {
                "title": "Tempo medio",
                "showTitle": True,
                "description": "",
            },
        }
        print(f"Table restored: average-time -> summary (filter-responsive, no column header)")

DASH.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")
print("Saved.")
