# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Orquestrador
# MAGIC Lê a camada `raw` do ADLS e executa a ingestão Bronze uma vez por fonte. Gera run_id/batch_id e propaga pelas camadas via taskValues.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import re
import uuid
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
)

dbutils.widgets.text("raw_dir", "", "raw_dir")

# COMMAND ----------

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)
storage = config["azure"]["storage_account"]
container = config["azure"]["container"]

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark = SparkSession.builder.appName("BronzeOrchestrator").getOrCreate()
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

raw_dir = dbutils.widgets.get("raw_dir")
if not raw_dir:
    raw_dir = f"abfss://{container}@{storage}.dfs.core.windows.net/raw"
raw_dir = raw_dir.rstrip("/")

files = [f for f in dbutils.fs.ls(raw_dir) if f.path.endswith(".csv")]

# Gera run_id/batch_id para rastrear o fluxo end-to-end
run_id = str(uuid.uuid4())
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
    print(f"run_id={run_id} / batch_id={batch_id} propagados via taskValues.")
except Exception as e:
    print(f"taskValues indisponível (execução manual?): {e}")


def log_data_quality(
    layer,
    step,
    source=None,
    year=None,
    row_count_in=None,
    row_count_out=None,
    rejected_records=None,
    key_columns_null_pct=None,
    schema_drift_flag=None,
    execution_time_sec=None,
    status="PASS",
    details=None,
):
    monitoring_path = f"abfss://{container}@{storage}.dfs.core.windows.net/monitoring/data_quality_log"
    null_pct_json = json.dumps(key_columns_null_pct) if key_columns_null_pct else None

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("step", StringType(), False),
            StructField("source", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("row_count_in", LongType(), True),
            StructField("row_count_out", LongType(), True),
            StructField("rejected_records", LongType(), True),
            StructField("key_columns_null_pct_json", StringType(), True),
            StructField("schema_drift_flag", BooleanType(), True),
            StructField("execution_time_sec", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("details", StringType(), True),
            StructField("recorded_at", TimestampType(), False),
        ]
    )

    row = [(
        run_id,
        batch_id,
        layer,
        step,
        source,
        int(year) if year is not None else None,
        row_count_in,
        row_count_out,
        rejected_records,
        null_pct_json,
        schema_drift_flag,
        execution_time_sec,
        status,
        details,
        datetime.now(timezone.utc),
    )]
    df = spark.createDataFrame(row, schema=schema)

    try:
        existing = spark.table("monitoring.data_quality_log")
        combined = existing.unionByName(df, allowMissingColumns=False)
    except Exception:
        combined = df

    try:
        dbutils.fs.rm(monitoring_path, recurse=True)
    except Exception:
        pass

    combined.write.format("delta").mode("overwrite").option("path", monitoring_path).saveAsTable(
        "monitoring.data_quality_log"
    )


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
            "run_id": run_id,
            "batch_id": batch_id,
        },
    )

# COMMAND ----------

log_data_quality(
    layer="bronze",
    step="bronze_orchestrator",
    row_count_in=len(files),
    row_count_out=len(sources),
    details=f"Fontes processadas: {list(sources.keys())}",
)

print(f"Orquestrador concluído. {len(sources)} fontes processadas.")
