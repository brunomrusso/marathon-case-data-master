# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Agregações
# MAGIC Geração das tabelas Gold para alimentar o dashboard final. Usa `silver.marathons_with_weather` quando disponível. Loga métricas de qualidade e rastreia run_id/batch_id.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import sys
import time
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from builtins import sum as builtins_sum

from pyspark.sql.functions import (
    col, count, avg, when, expr
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
)

# COMMAND ----------

spark = SparkSession.builder.appName("GoldAggregations").getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)
storage = config["azure"]["storage_account"]
container = config["azure"]["container"]

# Recupera run_id/batch_id propagado do orquestrador
run_id = "manual"
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
try:
    run_id = dbutils.jobs.taskValues.get(taskKey="bronze_orchestrator", key="run_id", default="manual", debugValue="manual")
    batch_id = dbutils.jobs.taskValues.get(taskKey="bronze_orchestrator", key="batch_id", default=batch_id, debugValue=batch_id)
    print(f"run_id={run_id} / batch_id={batch_id} recuperados do orquestrador.")
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

expected_gold_tables = {
    "kpi_summary", "finishers_by_year", "top_countries", "athletes_by_country",
    "times_distribution", "marathon_comparison", "age_gender_profile", "weather_impact"
}


def save_gold(df, table, partition_cols=None):
    gold_path = f"abfss://{container}@{storage}.dfs.core.windows.net/gold/{table}"
    try:
        dbutils.fs.rm(gold_path, recurse=True)
    except Exception:
        pass
    writer = (df.write
              .format("delta")
              .mode("overwrite")
              .option("path", gold_path))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"gold.{table}")


# Usa Silver enriquecida com clima; se ainda não existir, cai de volta para silver.marathons
silver = (
    spark.table("silver.marathons_with_weather")
    if "marathons_with_weather" in [t.name for t in spark.catalog.listTables("silver")]
    else spark.table("silver.marathons")
)

row_count_in = silver.count()

# COMMAND ----------

# 1. gold.kpi_summary
kpi_summary = (silver
    .groupBy("source", "year", "marathon_name")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec"),
        spark_min("finish_time_sec").alias("record_time_sec"),
        spark_sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_count"),
        spark_sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_count")
    )
    .withColumn("female_pct", spark_round(col("female_count") / col("total_athletes") * 100, 2)))

save_gold(kpi_summary, "kpi_summary", ["source", "year"])

# COMMAND ----------

# 2. gold.finishers_by_year
finishers_by_year = (silver
    .groupBy("source", "year", "marathon_name")
    .agg(
        count("*").alias("total_finishers"),
        spark_sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_finishers"),
        spark_sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_finishers")
    )
    .withColumn("female_pct", spark_round(col("female_finishers") / col("total_finishers") * 100, 2)))

save_gold(finishers_by_year, "finishers_by_year", ["source", "year"])

# COMMAND ----------

# 3. gold.top_countries
top_countries = (silver
    .filter(col("country").isNotNull())
    .groupBy("source", "year", "country")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

save_gold(top_countries, "top_countries", ["source", "year"])

# COMMAND ----------

# 4. gold.athletes_by_country
athletes_by_country = (silver
    .filter(col("country").isNotNull())
    .groupBy("country")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

save_gold(athletes_by_country, "athletes_by_country")

# COMMAND ----------

# 5. gold.times_distribution
times_distribution = (silver
    .filter(col("finish_time_sec").isNotNull())
    .groupBy("source", "year", "gender", "age_group")
    .agg(
        spark_min("finish_time_sec").alias("min"),
        spark_max("finish_time_sec").alias("max"),
        avg("finish_time_sec").alias("mean"),
        expr("percentile_approx(finish_time_sec, 0.5)").alias("median"),
        expr("percentile_approx(finish_time_sec, 0.25)").alias("q1"),
        expr("percentile_approx(finish_time_sec, 0.75)").alias("q3")
    ))

save_gold(times_distribution, "times_distribution", ["source", "year"])

# COMMAND ----------

# 6. gold.marathon_comparison
marathon_comparison = (silver
    .filter(col("finish_time_sec").isNotNull())
    .groupBy("year")
    .pivot("source")
    .agg(
        count("*").alias("finishers"),
        avg("finish_time_sec").alias("avg_time")
    ))

save_gold(marathon_comparison, "marathon_comparison", ["year"])

# COMMAND ----------

# 7. gold.age_gender_profile
age_gender_profile = (silver
    .groupBy("source", "year", "age_group", "gender")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

save_gold(age_gender_profile, "age_gender_profile", ["source", "year"])

# COMMAND ----------

# 8. gold.weather_impact (só quando há dados de clima)
weather_impact_created = False
try:
    weather_cols = [c for c in silver.columns if c in [
        "temperature_max_c", "temperature_min_c", "temperature_mean_c",
        "apparent_temperature_max_c", "precipitation_mm", "windspeed_max_kmh"
    ]]
    if weather_cols:
        weather_impact = (silver
            .filter(col("finish_time_sec").isNotNull())
            .groupBy("source", "year", "marathon_name", *weather_cols)
            .agg(
                count("*").alias("total_athletes"),
                avg("finish_time_sec").alias("avg_finish_time_sec"),
                spark_min("finish_time_sec").alias("record_time_sec")
            ))
        save_gold(weather_impact, "weather_impact", ["source", "year"])
        weather_impact_created = True
        print("Tabela gold.weather_impact gerada.")
    else:
        print("Dados de clima não disponíveis; gold.weather_impact ignorada.")
except Exception as e:
    print(f"Ignorando weather_impact: {e}")

# COMMAND ----------

# Schema drift: verifica se todas as tabelas Gold esperadas foram criadas
existing_gold_tables = {t.name for t in spark.catalog.listTables("gold")}
schema_drift_flag = not expected_gold_tables.issubset(existing_gold_tables)
if schema_drift_flag:
    print(f"ALERTA: faltam tabelas Gold. Esperadas: {expected_gold_tables} / Criadas: {existing_gold_tables}")

# Calcula % de nulos em colunas-chave da Silver (usada como input)
key_cols = ["gender", "finish_time_sec", "country"]
null_pct = {}
for c in key_cols:
    if c in silver.columns:
        total = row_count_in
        null_count = silver.filter(col(c).isNull()).count() if total > 0 else 0
        null_pct[c] = round(null_count / total * 100, 2) if total > 0 else 0.0

execution_time = time.time() - start_time

log_data_quality(
    layer="gold",
    step="gold_aggregations",
    row_count_in=row_count_in,
    row_count_out=builtins_sum([spark.table(f"gold.{t}").count() for t in existing_gold_tables if t in expected_gold_tables]),
    key_columns_null_pct=null_pct,
    schema_drift_flag=schema_drift_flag,
    execution_time_sec=round(execution_time, 2),
    status="WARN" if schema_drift_flag else "PASS",
    details=f"Tabelas Gold criadas: {existing_gold_tables}; weather_impact: {weather_impact_created}",
)

print("Tabelas Gold geradas com sucesso.")
