#!/usr/bin/env python3
"""Fix country-performance-table: use pre-aggregated dataset by country
with weighted-average time in HH:MM format."""
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
DASH = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"

d = json.loads(DASH.read_text(encoding="utf-8"))

# New dataset: pre-aggregate by country with weighted avg time in HH:MM
# Uses SUM(athletes * time_sec) / SUM(athletes) for correct weighted average
new_query = (
    "SELECT country, "
    "SUM(total_athletes) AS total_athletes, "
    "CONCAT("
    "  LPAD(CAST(FLOOR(SUM(total_athletes * avg_finish_time_sec) / SUM(total_athletes) / 3600) AS STRING), 2, '0'), ':', "
    "  LPAD(CAST(FLOOR(PMOD(SUM(total_athletes * avg_finish_time_sec) / SUM(total_athletes), 3600) / 60) AS STRING), 2, '0')"
    ") AS avg_finish_time_hm, "
    "CAST(SUM(total_athletes * avg_finish_time_sec) / SUM(total_athletes) / 60 AS DECIMAL(6,1)) AS avg_finish_time_min "
    "FROM top_countries WHERE country IS NOT NULL GROUP BY country HAVING SUM(total_athletes) >= 1000 "
    "ORDER BY avg_finish_time_min ASC"
)

for ds in d["datasets"]:
    if ds["name"] == "country_performance":
        ds["queryLines"] = [new_query]
        print(f"Dataset updated: {new_query[:80]}...")

# Fix widget: disaggregated=True, HH:MM column
for p in d["pages"]:
    for wi in p.get("layout", []):
        w = wi["widget"]
        if w.get("name") == "country-performance-table":
            w["queries"] = [{"name": "main_query", "query": {
                "datasetName": "country_performance",
                "fields": [
                    {"name": "country", "expression": "country"},
                    {"name": "total_athletes", "expression": "total_athletes"},
                    {"name": "avg_finish_time_hm", "expression": "avg_finish_time_hm"},
                ],
                "disaggregated": True,
            }}]
            w["spec"]["encodings"]["columns"] = [
                {"fieldName": "country", "displayName": "Pais"},
                {"fieldName": "total_athletes", "displayName": "Atletas"},
                {"fieldName": "avg_finish_time_hm", "displayName": "Tempo medio"},
            ]
            print("Widget fixed: disaggregated=True, HH:MM column")

DASH.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")
print("Saved.")
