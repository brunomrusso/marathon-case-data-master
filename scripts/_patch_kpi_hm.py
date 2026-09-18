#!/usr/bin/env python3
"""Patch dashboard: replace average-time counter with table widget showing HH:MM string."""
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
DASH = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"

d = json.loads(DASH.read_text(encoding="utf-8"))

# 1. Update kpi_global dataset to include formatted HH:MM string
HM_EXPR = (
    "SELECT "
    "CAST(SUM(total_athletes) AS BIGINT) AS total_finishers, "
    "COUNT(DISTINCT year) AS total_editions, "
    "COUNT(DISTINCT source) AS total_marathons, "
    "CONCAT("
    "  CAST(FLOOR(AVG(avg_finish_time_sec) / 3600) AS INT), 'h ', "
    "  LPAD(CAST(FLOOR(MOD(AVG(avg_finish_time_sec), 3600) / 60) AS INT), 2, '0'), 'm'"
    ") AS avg_finish_time_hm "
    "FROM kpi_summary "
    "WHERE avg_finish_time_sec IS NOT NULL"
)

for ds in d["datasets"]:
    if ds["name"] == "kpi_global":
        ds["queryLines"] = [HM_EXPR]
        print("kpi_global updated with avg_finish_time_hm string (HH:MM)")

# 2. Replace counter widget with table widget for average-time
ov = next(p for p in d["pages"] if p["name"] == "overview")
for wi in ov["layout"]:
    w = wi["widget"]
    if w.get("name") == "average-time":
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "kpi_global",
                "fields": [{"name": "avg_finish_time_hm", "expression": "avg_finish_time_hm"}],
                "disaggregated": True,
            },
        }]
        w["spec"] = {
            "version": 3,
            "widgetType": "table",
            "encodings": {
                "columns": [{
                    "booleanTrueLabel": "yes",
                    "booleanFalseLabel": "no",
                    "visible": True,
                    "fieldName": "avg_finish_time_hm",
                    "title": "",
                    "type": "string",
                    "displayAs": "string",
                    "alignContent": "center",
                }]
            },
            "invisibleColumns": [],
            "condensed": True,
            "withRowNumber": False,
            "frame": {
                "title": "Tempo medio",
                "showTitle": True,
                "description": "",
            },
        }
        print("average-time: converted to table widget (HH:MM display)")

DASH.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")
print("Saved.")
