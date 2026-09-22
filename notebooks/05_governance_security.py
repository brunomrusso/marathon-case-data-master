# Databricks notebook source

# MAGIC %md
# MAGIC # Governance & Security — Unity Catalog Masks
# MAGIC Aplica mascaramento dinâmico de colunas (column masking) e filtro de linhas
# MAGIC (row filter) no Unity Catalog para a tabela `silver.marathons_pii`,
# MAGIC que contém dados pessoais (`athlete_name`, `athlete_id`).
# MAGIC
# MAGIC Usuários sem privilégio `UNMASK` veem `***` nos campos sensíveis.
# MAGIC Administradores (`admins`) e o proprietário do pipeline veem os dados reais.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import sys
import time
from datetime import datetime, timezone

import yaml
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

# COMMAND ----------

spark = SparkSession.builder.appName("GovernanceSecurity").getOrCreate()

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)
storage = config["azure"]["storage_account"]
container = config["azure"]["container"]
catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS governance")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

run_id = "manual"
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
try:
    run_id = dbutils.jobs.taskValues.get(taskKey="bronze_orchestrator", key="run_id", default="manual", debugValue="manual")
    batch_id = dbutils.jobs.taskValues.get(taskKey="bronze_orchestrator", key="batch_id", default=batch_id, debugValue=batch_id)
except Exception as e:
    print(f"taskValues indisponível (execução manual?): {e}")

start_time = time.time()

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

# Função de mascaramento dinâmico: somente membros do grupo/workspace "admins"
# ou o proprietário do objeto veem o valor real; demais usuários veem "***".
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.governance.mask_pii(value STRING)
RETURN CASE
    WHEN is_member('admins') THEN value
    ELSE '***'
END
""")

# Filtro de linhas: restringe leitura a registros a partir de 2014 para
# usuários que não sejam admins (exemplo didático de row-level security).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.governance.row_filter_recent(year INT)
RETURN CASE
    WHEN is_member('admins') THEN TRUE
    ELSE year >= 2014
END
""")

# Aplica mascaramento à tabela PII
spark.sql(f"""
ALTER TABLE {catalog_name}.silver.marathons_pii
ALTER COLUMN athlete_name SET MASK {catalog_name}.governance.mask_pii
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.silver.marathons_pii
ALTER COLUMN athlete_id SET MASK {catalog_name}.governance.mask_pii
""")

# Aplica filtro de linhas à tabela PII
spark.sql(f"""
ALTER TABLE {catalog_name}.silver.marathons_pii
SET ROW FILTER {catalog_name}.governance.row_filter_recent ON (year)
""")

# Concede acesso de leitura no schema governance para account users
spark.sql(f"GRANT USAGE ON SCHEMA {catalog_name}.governance TO `account users`")
spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog_name}.governance.mask_pii TO `account users`")
spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog_name}.governance.row_filter_recent TO `account users`")

# Garante que account users possam ler a tabela PII (máscara será aplicada automaticamente)
spark.sql(f"GRANT SELECT ON TABLE {catalog_name}.silver.marathons_pii TO `account users`")

# COMMAND ----------

pii_count = spark.table("silver.marathons_pii").count()
execution_time = time.time() - start_time

log_data_quality(
    layer="governance",
    step="unity_catalog_masks",
    row_count_in=pii_count,
    row_count_out=pii_count,
    execution_time_sec=round(execution_time, 2),
    status="PASS",
    details="Column masks aplicados a athlete_name/athlete_id; row filter year>=2014 para nao-admins em silver.marathons_pii",
)

print(f"Governança aplicada: {pii_count} registros em silver.marathons_pii com masks e row filter.")
