# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Orquestrador
# MAGIC Varre a pasta raw do DBFS e executa a ingestão Bronze uma vez por fonte.
# MAGIC Para fontes multi-arquivo (London), usa glob para ler todos de uma vez.

# COMMAND ----------

import re

dbutils.widgets.text("raw_dir", "dbfs:/FileStore/marathon/raw", "raw_dir")

# COMMAND ----------

raw_dir = dbutils.widgets.get("raw_dir")
files = [f for f in dbutils.fs.ls(raw_dir) if f.path.endswith(".csv")]

def classify(file_name):
    lower = file_name.lower()
    if "chicago" in lower:
        return "chicago", ";"
    if lower.startswith("london_"):
        return "london", ","
    if "nyc" in lower or "new_york" in lower:
        return "nyc", ","
    if "berlin" in lower:
        return "berlin", ";"
    raise ValueError(f"Fonte desconhecida para o arquivo: {file_name}")

# COMMAND ----------

# Agrupa arquivos por fonte
sources = {}
for f in files:
    source, delimiter = classify(f.name)
    sources.setdefault(source, {"delimiter": delimiter, "paths": []})
    sources[source]["paths"].append(f.path)

# Chama o 01 uma vez por fonte. Para London, passa um glob.
for source, info in sources.items():
    if source == "london":
        file_path = f"{raw_dir}/London_*.csv"
    else:
        file_path = info["paths"][0]

    print(f"Ingerindo: {source} -> {file_path} (delimiter={info['delimiter']})")
    dbutils.notebook.run(
        "01_bronze_ingestion",
        3600,
        {
            "source": source,
            "year": "0",
            "file_path": file_path,
            "delimiter": info["delimiter"],
        },
    )

# COMMAND ----------

print(f"Orquestrador concluído. {len(sources)} fontes processadas.")
