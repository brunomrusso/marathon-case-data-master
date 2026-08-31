# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Orquestrador
# MAGIC Varre a pasta raw do DBFS e executa a ingestão Bronze para cada CSV encontrado.

# COMMAND ----------

import re

dbutils.widgets.text("raw_dir", "dbfs:/FileStore/marathon/raw", "raw_dir")

# COMMAND ----------

raw_dir = dbutils.widgets.get("raw_dir")
files = [f for f in dbutils.fs.ls(raw_dir) if f.path.endswith(".csv")]

def classify(file_name):
    lower = file_name.lower()
    if "chicago" in lower:
        return ("chicago", 0, ";")
    if lower.startswith("london_"):
        m = re.search(r"london_(\d{4})_", lower)
        year = int(m.group(1)) if m else 0
        return ("london", year, ",")
    if "nyc" in lower or "new_york" in lower:
        return ("nyc", 0, ",")
    if "berlin" in lower:
        return ("berlin", 0, ";")
    raise ValueError(f"Fonte desconhecida para o arquivo: {file_name}")

# COMMAND ----------

for file_info in files:
    file_name = file_info.name
    file_path = file_info.path
    source, year, delimiter = classify(file_name)
    print(f"Ingerindo: {file_name} -> source={source}, year={year}, delimiter={delimiter}")
    dbutils.notebook.run(
        "01_bronze_ingestion",
        3600,
        {
            "source": source,
            "year": str(year),
            "file_path": file_path,
            "delimiter": delimiter,
        },
    )

# COMMAND ----------

print(f"Orquestrador concluído. {len(files)} arquivos processados.")
