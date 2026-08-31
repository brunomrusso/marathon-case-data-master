# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Ingestão de CSVs
# MAGIC Ingestão incremental dos arquivos CSV brutos para tabelas Delta na camada Bronze.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import re
import sys
import yaml
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, input_file_name, regexp_extract
)

# COMMAND ----------

sys.path.insert(0, "../src")

# COMMAND ----------

dbutils.widgets.text("source", "")
dbutils.widgets.text("year", "")
dbutils.widgets.text("file_path", "")
dbutils.widgets.text("delimiter", ",")

# COMMAND ----------

source = dbutils.widgets.get("source")
year = int(dbutils.widgets.get("year"))
file_path = dbutils.widgets.get("file_path")
delimiter = dbutils.widgets.get("delimiter")

# COMMAND ----------

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)

storage = config["azure"]["storage_account"]
container = config["azure"]["container"]
bronze_path = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/{source}"

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", delimiter)
      .csv(file_path))

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

# Remove duplicatas no source antes do merge
total_raw = df.count()
df = df.dropDuplicates(["row_hash"])
total_dedup = df.count()

if total_raw != total_dedup:
    print(f"Removidas {total_raw - total_dedup} linhas duplicadas de {source}: {total_raw} -> {total_dedup}")

# COMMAND ----------

from delta.tables import DeltaTable

bronze_table = f"bronze.{source}"

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

print(f"Ingestao concluida: {source} — {total_dedup} registros.")
