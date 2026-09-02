# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Ingestão de CSVs
# MAGIC Ingestão incremental dos arquivos CSV brutos para tabelas Delta na camada Bronze. Rastreia run_id/batch_id e loga métricas de qualidade.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import re
import sys
import time
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, input_file_name, regexp_extract
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
)

# COMMAND ----------

sys.path.insert(0, "../src")

# COMMAND ----------

dbutils.widgets.text("source", "")
dbutils.widgets.text("year", "")
dbutils.widgets.text("file_path", "")
dbutils.widgets.text("delimiter", ",")
dbutils.widgets.text("run_id", "manual", "run_id")
dbutils.widgets.text("batch_id", "manual", "batch_id")

# COMMAND ----------

source = dbutils.widgets.get("source")
year = int(dbutils.widgets.get("year"))
file_path = dbutils.widgets.get("file_path")
delimiter = dbutils.widgets.get("delimiter")
run_id = dbutils.widgets.get("run_id")
batch_id = dbutils.widgets.get("batch_id")

start_time = time.time()

# COMMAND ----------

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)

storage = config["azure"]["storage_account"]
container = config["azure"]["container"]
bronze_path = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/{source}"

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------


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

    df.write.format("delta").mode("append").option("mergeSchema", "true").option(
        "path", monitoring_path
    ).saveAsTable("monitoring.data_quality_log")


# COMMAND ----------

# Detecta schema drift comparando com o schema existente da tabela Bronze
previous_schema_cols = None
bronze_table = f"bronze.{source}"
if spark.catalog.tableExists(bronze_table):
    previous_schema_cols = set(spark.table(bronze_table).columns)

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", delimiter)
      .csv(file_path))

current_schema_cols = set(df.columns)
schema_drift_flag = False
if previous_schema_cols and current_schema_cols != previous_schema_cols:
    schema_drift_flag = True
    print(f"ALERTA: schema drift detectado em {source}. Anterior: {previous_schema_cols} / Atual: {current_schema_cols}")

# Preserva o ano do CSV quando existe (Year/year), senao extrai do nome do arquivo ou do widget
year_col = next((c for c in df.columns if c.lower() == "year"), None)
if year_col is None:
    if "*" in file_path:
        df = df.withColumn("year", regexp_extract(input_file_name(), r"_(\d{4})", 1).cast("int"))
    else:
        df = df.withColumn("year", lit(year).cast("int"))
else:
    if year_col != "year":
        df = df.withColumnRenamed(year_col, "year")
    df = df.withColumn("year", col("year").cast("int"))

# Sanitiza nomes de colunas para evitar caracteres invalidos no Delta


def sanitize(name):
    name = re.sub(r"[ ,;{}()\n\t=]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name if name else "_col"


new_cols = [sanitize(c) for c in df.columns]
df = df.toDF(*new_cols)

cols = [c for c in df.columns]

# Identifica o nome do arquivo por linha
if "*" in file_path:
    file_name_col = input_file_name()
else:
    file_name_col = lit(file_path)

df = (df
      .withColumn("source", lit(source))
      .withColumn("ingestion_date", current_timestamp())
      .withColumn("row_hash", sha2(concat_ws("||", *cols), 256))
      .withColumn("file_name", file_name_col))

# Calcula % de nulos em colunas-chave (antes da deduplicacao)
key_cols = ["year", "row_hash"]
null_pct = {}
for c in key_cols:
    if c in df.columns:
        null_count = df.filter(col(c).isNull() | (col(c) == "")).count()
        total = df.count()
        null_pct[c] = round(null_count / total * 100, 2) if total > 0 else 0.0

# Remove duplicatas no source antes do merge
total_raw = df.count()
df = df.dropDuplicates(["row_hash"])
total_dedup = df.count()

if total_raw != total_dedup:
    print(f"Removidas {total_raw - total_dedup} linhas duplicadas de {source}: {total_raw} -> {total_dedup}")

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(bronze_table):
    try:
        dbutils.fs.rm(bronze_path, recurse=True)
    except Exception:
        pass
    (df.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("year")
     .option("path", bronze_path)
     .saveAsTable(bronze_table))
else:
    delta_table = DeltaTable.forName(spark, bronze_table)
    (delta_table.alias("t")
     .merge(df.alias("s"), "t.source = s.source AND t.year = s.year AND t.row_hash = s.row_hash")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

file_meta_df = (df.groupBy("year", "file_name")
                 .count()
                 .withColumn("source", lit(source))
                 .withColumn("ingestion_date", current_timestamp())
                 .withColumn("year", col("year").cast("int"))
                 .withColumn("rows", col("count").cast("long"))
                 .select("source", "year", "file_name", "rows", "ingestion_date"))

if not spark.catalog.tableExists("bronze.file_metadata"):
    file_meta_df.write.format("delta").mode("overwrite").saveAsTable("bronze.file_metadata")
else:
    file_meta_df.write.format("delta").mode("append").saveAsTable("bronze.file_metadata")

# COMMAND ----------

execution_time = time.time() - start_time

log_data_quality(
    layer="bronze",
    step="bronze_ingestion",
    source=source,
    year=year if year != 0 else None,
    row_count_in=total_raw,
    row_count_out=total_dedup,
    rejected_records=total_raw - total_dedup,
    key_columns_null_pct=null_pct,
    schema_drift_flag=schema_drift_flag,
    execution_time_sec=round(execution_time, 2),
    status="WARN" if schema_drift_flag else "PASS",
    details=f"Tabela {bronze_table} atualizada. Schema drift: {schema_drift_flag}",
)

print(f"Ingestao concluida: {source} — {total_dedup} registros.")

