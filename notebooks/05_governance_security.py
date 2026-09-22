# Databricks notebook source

# MAGIC %md
# MAGIC # Governance & Security — Visão Mascarada de Dados Pessoais
# MAGIC A tabela `silver.marathons_pii` contém dados pessoais (`athlete_name`,
# MAGIC `athlete_id`). Como o job cluster do case é single-user e o Unity Catalog
# MAGIC só aplica column masks/row filters em shared clusters, criamos uma
# MAGIC **view mascarada** `silver.marathons_pii_public` para consumo geral.
# MAGIC
# MAGIC A view usa `is_member('admins')` para decidir se expõe o valor real ou `***`.
# MAGIC Em produção, com shared cluster, a boa prática é usar column masks e row
# filters nativos do Unity Catalog.

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

# View pública: campos pessoais ficam '***' para usuários comuns;
# administradores (grupo 'admins') veem os valores reais.
# Essa abordagem funciona em single-node clusters, onde column masks nativas
# do Unity Catalog não são suportadas.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.silver.marathons_pii_public
AS
SELECT
    source,
    year,
    marathon_name,
    CASE WHEN is_member('admins') THEN athlete_name ELSE '***' END AS athlete_name,
    CASE WHEN is_member('admins') THEN athlete_id ELSE '***' END AS athlete_id,
    gender,
    age_group,
    country,
    finish_time,
    finish_time_sec,
    half_time,
    half_time_sec,
    place_overall,
    place_gender,
    club
FROM {catalog_name}.silver.marathons_pii
WHERE CASE WHEN is_member('admins') THEN TRUE ELSE year >= 2014 END
""")

# View administrativa: acesso completo a todos os registros e campos pessoais
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.silver.marathons_pii_admin
AS
SELECT * FROM {catalog_name}.silver.marathons_pii
""")

# Concede acesso às views (a tabela base continua restrita por padrão)
spark.sql(f"GRANT SELECT ON TABLE {catalog_name}.silver.marathons_pii_public TO `account users`")
spark.sql(f"GRANT SELECT ON TABLE {catalog_name}.silver.marathons_pii_admin TO `admins`")

# COMMAND ----------

pii_count = spark.table("silver.marathons_pii").count()
execution_time = time.time() - start_time

log_data_quality(
    layer="governance",
    step="masked_pii_views",
    row_count_in=pii_count,
    row_count_out=pii_count,
    execution_time_sec=round(execution_time, 2),
    status="PASS",
    details="Views silver.marathons_pii_public (masked) e silver.marathons_pii_admin (full) criadas. Em shared clusters, substituir por column masks/row filters nativos do Unity Catalog.",
)

print(f"Governança aplicada: {pii_count} registros em silver.marathons_pii; views public/admin criadas.")
