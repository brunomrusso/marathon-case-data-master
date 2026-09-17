#!/usr/bin/env python3
"""Fix dashboard: normalize source case, fix avg time counter, filterable countries, remove quality."""
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
DASH = ROOT / "dashboard" / "databricks" / "marathon_dashboard.lvdash.json"

SRC_NORM = (
    "CASE source "
    "WHEN 'berlin' THEN 'Berlin' "
    "WHEN 'chicago' THEN 'Chicago' "
    "WHEN 'london' THEN 'London' "
    "WHEN 'nyc' THEN 'New York' "
    "ELSE source END"
)

GENDER_FILTER = "gender IN ('F','M','X','NON-BINARY','NOT SPECIFIED')"

with open(DASH, encoding="utf-8") as f:
    d = json.load(f)

# ── 1. FIX DATASETS ──────────────────────────────────────────────────────────

for ds in d["datasets"]:
    if ds["name"] == "summary":
        ds["queryLines"] = [
            "SELECT source, year, SUM(total_athletes) AS total_finishers, "
            "COUNT(DISTINCT year) AS total_editions, "
            "COUNT(DISTINCT source) AS total_marathons, "
            "ROUND(AVG(avg_finish_time_sec) / 60, 1) AS avg_finish_time_min "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, total_athletes, avg_finish_time_sec "
            "FROM kpi_summary"
            ") GROUP BY source, year"
        ]

    elif ds["name"] == "finishers_by_year":
        ds["queryLines"] = [
            "SELECT year, source, SUM(total_finishers) AS total_finishers "
            "FROM ("
            f"SELECT year, {SRC_NORM} AS source, total_finishers FROM finishers_by_year"
            ") GROUP BY year, source ORDER BY year, source"
        ]

    elif ds["name"] == "top_countries":
        ds["queryLines"] = [
            "SELECT source, year, country, SUM(total_athletes) AS total_athletes "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, country, total_athletes "
            "FROM top_countries WHERE country IS NOT NULL"
            ") GROUP BY source, year, country ORDER BY total_athletes DESC LIMIT 20"
        ]

    elif ds["name"] == "demographics":
        ds["queryLines"] = [
            "SELECT source, year, age_group, gender, "
            "SUM(total_athletes) AS total_athletes, "
            "ROUND(AVG(avg_finish_time_sec) / 60, 1) AS avg_finish_time_min "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, age_group, gender, "
            "total_athletes, avg_finish_time_sec "
            f"FROM age_gender_profile WHERE age_group IS NOT NULL AND {GENDER_FILTER}"
            ") GROUP BY source, year, age_group, gender "
            "ORDER BY CASE WHEN age_group = '0-17' THEN 0 "
            "WHEN age_group = '18-39' THEN 18 "
            "ELSE CAST(SPLIT(age_group, '-')[0] AS INT) END, gender"
        ]

    elif ds["name"] == "weather":
        ds["queryLines"] = [
            "SELECT source, year, temperature_mean_c, precipitation_mm, "
            "ROUND(avg_finish_time_sec / 60, 1) AS avg_finish_time_min "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, "
            "temperature_mean_c, precipitation_mm, avg_finish_time_sec "
            "FROM weather_impact WHERE temperature_mean_c IS NOT NULL"
            ")"
        ]

    elif ds["name"] == "country_performance":
        # Swap athletes_by_country -> top_countries so source+year are available for filters
        ds["queryLines"] = [
            "SELECT source, year, country, "
            "SUM(total_athletes) AS total_athletes, "
            "ROUND(AVG(avg_finish_time_sec) / 60, 1) AS avg_finish_time_min, "
            "CONCAT("
            "LPAD(CAST(FLOOR(AVG(avg_finish_time_sec) / 3600) AS STRING), 2, '0'), ':', "
            "LPAD(CAST(FLOOR(PMOD(AVG(avg_finish_time_sec), 3600) / 60) AS STRING), 2, '0')"
            ") AS avg_finish_time_hm "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, country, "
            "total_athletes, avg_finish_time_sec "
            "FROM top_countries WHERE country IS NOT NULL"
            ") GROUP BY source, year, country ORDER BY total_athletes DESC"
        ]

    elif ds["name"] == "times_metrics":
        ds["queryLines"] = [
            "SELECT source, year, gender, age_group, "
            "ROUND(mean / 60, 1) AS mean_time_min, "
            "CONCAT(LPAD(CAST(FLOOR(min / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(min, 3600) / 60) AS STRING), 2, '0')) AS min_time_hm, "
            "CONCAT(LPAD(CAST(FLOOR(q1 / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(q1, 3600) / 60) AS STRING), 2, '0')) AS q1_time_hm, "
            "CONCAT(LPAD(CAST(FLOOR(median / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(median, 3600) / 60) AS STRING), 2, '0')) AS median_time_hm, "
            "CONCAT(LPAD(CAST(FLOOR(mean / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(mean, 3600) / 60) AS STRING), 2, '0')) AS mean_time_hm, "
            "CONCAT(LPAD(CAST(FLOOR(q3 / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(q3, 3600) / 60) AS STRING), 2, '0')) AS q3_time_hm, "
            "CONCAT(LPAD(CAST(FLOOR(max / 3600) AS STRING), 2, '0'), ':', LPAD(CAST(FLOOR(PMOD(max, 3600) / 60) AS STRING), 2, '0')) AS max_time_hm "
            "FROM ("
            f"SELECT {SRC_NORM} AS source, year, gender, age_group, "
            "mean, min, q1, median, q3, max "
            "FROM times_distribution "
            f"WHERE age_group IS NOT NULL AND {GENDER_FILTER}"
            ")"
        ]

# Remove the 'quality' dataset entirely
d["datasets"] = [ds for ds in d["datasets"] if ds["name"] != "quality"]

print("Datasets:", [ds["name"] for ds in d["datasets"]])

# ── 2. FIX GLOBAL FILTERS ────────────────────────────────────────────────────
# Remove comparison references (cause duplicity); add country_performance refs

filters_page = next(p for p in d["pages"] if p["name"] == "global-filters")

for widget_item in filters_page["layout"]:
    w = widget_item["widget"]
    wname = w["name"]

    if wname == "filter-marathon":
        # Remove q_comparison_source, add q_country_source
        w["queries"] = [q for q in w["queries"] if q["name"] != "q_comparison_source"]
        w["queries"].append({
            "name": "q_country_source",
            "query": {
                "datasetName": "country_performance",
                "fields": [{"name": "source", "expression": "`source`"}],
                "disaggregated": False
            }
        })
        enc = w["spec"]["encodings"]["fields"]
        enc[:] = [e for e in enc if e["queryName"] != "q_comparison_source"]
        enc.append({"fieldName": "source", "displayName": "Maratona", "queryName": "q_country_source"})

    elif wname == "filter-year":
        # Remove q_comparison_year, add q_country_year
        w["queries"] = [q for q in w["queries"] if q["name"] != "q_comparison_year"]
        w["queries"].append({
            "name": "q_country_year",
            "query": {
                "datasetName": "country_performance",
                "fields": [{"name": "year", "expression": "`year`"}],
                "disaggregated": False
            }
        })
        enc = w["spec"]["encodings"]["fields"]
        enc[:] = [e for e in enc if e["queryName"] != "q_comparison_year"]
        enc.append({"fieldName": "year", "displayName": "Ano", "queryName": "q_country_year"})

# ── 3. FIX AVERAGE-TIME COUNTER (numeric minutes, not string CONCAT) ─────────
overview_page = next(p for p in d["pages"] if p["name"] == "overview")
for widget_item in overview_page["layout"]:
    w = widget_item["widget"]
    if w.get("name") == "average-time":
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "summary",
                "fields": [{
                    "name": "avg_finish_time_min",
                    "expression": "ROUND(AVG(`avg_finish_time_min`), 0)"
                }],
                "disaggregated": False
            }
        }]
        w["spec"] = {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {
                    "fieldName": "avg_finish_time_min",
                    "displayName": "min",
                    "format": {"type": "number", "precision": 0}
                }
            },
            "frame": {"title": "Tempo medio (min)", "showTitle": True}
        }
        print("Fixed average-time counter -> avg_finish_time_min (numeric)")

# ── 4. FIX COUNTRIES PAGE: aggregated widget queries ─────────────────────────
countries_page = next(p for p in d["pages"] if p["name"] == "countries-page")
for widget_item in countries_page["layout"]:
    w = widget_item["widget"]
    if w.get("name") == "country-athletes-chart":
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "country_performance",
                "fields": [
                    {"name": "country", "expression": "`country`"},
                    {"name": "total_athletes", "expression": "SUM(`total_athletes`)"}
                ],
                "disaggregated": False
            }
        }]
    elif w.get("name") == "country-performance-table":
        w["queries"] = [{
            "name": "main_query",
            "query": {
                "datasetName": "country_performance",
                "fields": [
                    {"name": "country", "expression": "`country`"},
                    {"name": "total_athletes", "expression": "SUM(`total_athletes`)"},
                    {"name": "avg_finish_time_min", "expression": "ROUND(AVG(`avg_finish_time_min`), 1)"}
                ],
                "disaggregated": False
            }
        }]
        w["spec"] = {
            "version": 2,
            "widgetType": "table",
            "encodings": {
                "columns": [
                    {"fieldName": "country", "displayName": "Pais"},
                    {"fieldName": "total_athletes", "displayName": "Atletas"},
                    {"fieldName": "avg_finish_time_min", "displayName": "Tempo medio (min)"}
                ]
            },
            "frame": {"title": "Ranking detalhado", "showTitle": True}
        }

# ── 5. FIX WEATHER-QUALITY PAGE: remove quality-table, resize weather-scatter ─
wq_page = next(p for p in d["pages"] if p["name"] == "weather-quality")

# Update title
for widget_item in wq_page["layout"]:
    w = widget_item["widget"]
    if w.get("name") == "weather-title":
        w["multilineTextboxSpec"]["lines"] = ["## Clima e performance"]

# Remove quality-table widget
wq_page["layout"] = [
    item for item in wq_page["layout"]
    if item["widget"].get("name") != "quality-table"
]

# Resize weather-scatter to full width
for widget_item in wq_page["layout"]:
    if widget_item["widget"].get("name") == "weather-scatter":
        widget_item["position"]["width"] = 12

# ── SAVE ─────────────────────────────────────────────────────────────────────
with open(DASH, "w", encoding="utf-8") as f:
    json.dump(d, f, indent=2, ensure_ascii=False)

print("Dashboard saved with all 5 fixes applied.")
