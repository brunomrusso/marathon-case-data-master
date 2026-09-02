# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — ETL
# MAGIC Limpeza, padronização de schema, integração das fontes, mascaramento/anonimização e log de qualidade. Rastreia run_id/batch_id via taskValues.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import sys
import time
import uuid
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, trim, regexp_extract, coalesce, sha2, concat_ws,
    concat, floor
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
)

# COMMAND ----------

sys.path.insert(0, "../src")

# COMMAND ----------

spark = SparkSession.builder.appName("SilverETL").getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)
storage = config["azure"]["storage_account"]
container = config["azure"]["container"]
silver_path = f"abfss://{container}@{storage}.dfs.core.windows.net/silver/marathons"

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

# Schema esperado da Silver para detectar drift
expected_silver_cols = {
    "source", "year", "marathon_name", "athlete_id_hash", "gender",
    "age_group", "country", "finish_time", "finish_time_sec",
    "half_time", "half_time_sec", "place_overall", "place_gender", "club"
}

# COMMAND ----------


def safe_get(df, source_name, alias_name):
    if source_name in df.columns:
        return col(source_name).alias(alias_name)
    return lit(None).alias(alias_name)


def normalize_gender(gender_col):
    return (when(upper(trim(col(gender_col))).isin(["MAN", "MALE", "M"]), "M")
            .when(upper(trim(col(gender_col))).isin(["WOMAN", "FEMALE", "W", "F"]), "F")
            .otherwise(upper(trim(col(gender_col))))
            .alias(gender_col))


def to_int(column):
    return (when(column.rlike(r"\d+"), regexp_extract(column, r"\d+", 0).cast("int"))
            .otherwise(lit(None).cast("int")))


def to_long(column):
    return (when(column.rlike(r"\d+"), regexp_extract(column, r"\d+", 0).cast("long"))
            .otherwise(lit(None).cast("long")))


def parse_time_to_seconds(time_col, output_name):
    h = regexp_extract(col(time_col), r"(\d+):", 1)
    m = regexp_extract(col(time_col), r"\d+:(\d+):", 1)
    s = regexp_extract(col(time_col), r"\d+:\d+:(\d+)", 1)
    return (when((h != "") & (m != "") & (s != ""),
                 h.cast("int") * 3600 + m.cast("int") * 60 + s.cast("int"))
            .otherwise(lit(None))
            .cast(LongType())
            .alias(output_name))


# COMMAND ----------

# Chicago
chicago = spark.table("bronze.chicago")
chicago_count_in = chicago.count()
chicago = (chicago
    .withColumn("source", lit("chicago"))
    .withColumn("marathon_name", lit("Chicago Marathon"))
    .withColumn("athlete_name", safe_get(chicago, "athlete_name", "athlete_name"))
    .withColumn("athlete_id", safe_get(chicago, "athlete_id", "athlete_id"))
    .withColumn("gender", normalize_gender("gender"))
    .withColumn("age_group", upper(trim(safe_get(chicago, "age_group", "age_group"))))
    .withColumn("country", upper(trim(safe_get(chicago, "country_ioc", "country"))))
    .withColumn("place_overall", to_int(safe_get(chicago, "place_overall", "place_overall")))
    .withColumn("place_gender", to_int(safe_get(chicago, "place_gender", "place_gender")))
    .withColumn("finish_time", safe_get(chicago, "finish_time", "finish_time"))
    .withColumn("half_time", lit(None).cast("string"))
    .withColumn("club", lit(None).cast("string"))
)
chicago = (chicago
    .withColumn("finish_time_sec", coalesce(
        to_long(col("finish_time_seconds")),
        parse_time_to_seconds("finish_time", "finish_time_sec")))
    .withColumn("half_time_sec", coalesce(
        to_long(col("half_split_seconds")),
        parse_time_to_seconds("half_time", "half_time_sec"))))

# London
london = spark.table("bronze.london")
london_count_in = london.count()
london = (london
    .withColumn("source", lit("london"))
    .withColumn("marathon_name", lit("London Marathon"))
    .withColumn("athlete_name", regexp_extract(trim(col("Name")), r"^(.+?) \(", 1))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("country", upper(regexp_extract(trim(col("Name")), r"\((\w{3})\)", 1)))
    .withColumn("gender", normalize_gender("Gender"))
    .withColumn("age_group", upper(trim(safe_get(london, "Category", "age_group"))))
    .withColumn("place_overall", to_int(safe_get(london, "Overall_Place", "place_overall")))
    .withColumn("place_gender", to_int(safe_get(london, "Gender_Place", "place_gender")))
    .withColumn("finish_time", safe_get(london, "Finish_Time", "finish_time"))
    .withColumn("half_time", safe_get(london, "Half_Time", "half_time"))
    .withColumn("club", safe_get(london, "Club", "club"))
)
london = (london
    .withColumn("finish_time_sec", parse_time_to_seconds("finish_time", "finish_time_sec"))
    .withColumn("half_time_sec", parse_time_to_seconds("half_time", "half_time_sec")))

# New York
nyc = spark.table("bronze.nyc")
nyc_count_in = nyc.count()
nyc = (nyc
    .withColumn("source", lit("nyc"))
    .withColumn("marathon_name", lit("New York City Marathon"))
    .withColumn("athlete_name", safe_get(nyc, "Name", "athlete_name"))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("gender", normalize_gender("Gender"))
    .withColumn("age_group", when(to_int(col("Age")).isNotNull(), concat(
        (floor(to_int(col("Age")) / 5) * 5).cast("int"),
        lit("-"),
        (floor(to_int(col("Age")) / 5) * 5 + 4).cast("int"))
        ).otherwise(None))
    .withColumn("country", upper(trim(safe_get(nyc, "Country", "country"))))
    .withColumn("place_overall", to_int(safe_get(nyc, "Overall", "place_overall")))
    .withColumn("place_gender", lit(None).cast("int"))
    .withColumn("finish_time", safe_get(nyc, "Finish_Time", "finish_time"))
    .withColumn("half_time", lit(None).cast("string"))
    .withColumn("club", lit(None).cast("string"))
)
nyc = (nyc
    .withColumn("finish_time_sec", coalesce(
        to_long(col("Finish")),
        parse_time_to_seconds("finish_time", "finish_time_sec")))
    .withColumn("half_time_sec", lit(None).cast(LongType())))

# Berlin
berlin = spark.table("bronze.berlin")
berlin_count_in = berlin.count()
berlin = (berlin
    .withColumn("source", lit("berlin"))
    .withColumn("marathon_name", lit("Berlin Marathon"))
    .withColumn("athlete_name", safe_get(berlin, "name", "athlete_name"))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("gender", normalize_gender("sex"))
    .withColumn("age_int", to_int(safe_get(berlin, "age_category", "age_category")))
    .withColumn("age_group", when(col("age_int").isNotNull(), concat(
        (floor(col("age_int") / 5) * 5).cast("int"),
        lit("-"),
        (floor(col("age_int") / 5) * 5 + 4).cast("int"))
        ).otherwise(upper(trim(safe_get(berlin, "age_category", "age_group")))))
    .withColumn("country", upper(trim(safe_get(berlin, "nation", "country"))))
    .withColumn("place_overall", lit(None).cast("int"))
    .withColumn("place_gender", lit(None).cast("int"))
    .withColumn("finish_time", safe_get(berlin, "final", "finish_time"))
    .withColumn("half_time", safe_get(berlin, "half", "half_time"))
    .withColumn("club", lit(None).cast("string"))
    .drop("age_int")
)
berlin = (berlin
    .withColumn("finish_time_sec", parse_time_to_seconds("finish_time", "finish_time_sec"))
    .withColumn("half_time_sec", parse_time_to_seconds("half_time", "half_time_sec")))

# COMMAND ----------

common_cols = [
    "source", "year", "marathon_name", "athlete_id", "athlete_name",
    "gender", "age_group", "country", "finish_time", "finish_time_sec",
    "half_time", "half_time_sec", "place_overall", "place_gender", "club"
]

union_df = (chicago.select(common_cols)
            .unionByName(london.select(common_cols))
            .unionByName(nyc.select(common_cols))
            .unionByName(berlin.select(common_cols)))

row_count_in = chicago_count_in + london_count_in + nyc_count_in + berlin_count_in

# COMMAND ----------

# Mascaramento/anonimização
union_df = (union_df
    .withColumn("athlete_id_hash", sha2(concat_ws("||", "source", "year",
        coalesce(col("athlete_id"), col("athlete_name"), col("place_overall").cast("string"))), 256))
    .drop("athlete_name")
    .drop("athlete_id"))

# COMMAND ----------

# Validações básicas: remove registros sem gênero ou com tempo negativo
invalid_count = union_df.filter(col("gender").isNull() | (col("finish_time_sec") < 0)).count()
if invalid_count > 0:
    print(f"Removidos {invalid_count} registros invalidos da Silver (gender nulo ou finish_time_sec < 0)")

union_df = union_df.filter(col("gender").isNotNull() & ((col("finish_time_sec") >= 0) | col("finish_time_sec").isNull()))
row_count_out = union_df.count()

# % de nulos em colunas-chave
key_cols = ["gender", "finish_time_sec", "country", "age_group"]
null_pct = {}
for c in key_cols:
    if c in union_df.columns:
        total = row_count_out
        null_count = union_df.filter(col(c).isNull()).count() if total > 0 else 0
        null_pct[c] = round(null_count / total * 100, 2) if total > 0 else 0.0

# Schema drift: compara com colunas esperadas da Silver
schema_drift_flag = set(union_df.columns) != expected_silver_cols
if schema_drift_flag:
    print(f"ALERTA: schema drift na Silver. Esperado: {expected_silver_cols} / Atual: {set(union_df.columns)}")

# COMMAND ----------

try:
    dbutils.fs.rm(silver_path, recurse=True)
except Exception:
    pass

(union_df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .option("path", silver_path)
 .saveAsTable("silver.marathons"))

execution_time = time.time() - start_time

log_data_quality(
    layer="silver",
    step="silver_etl",
    row_count_in=row_count_in,
    row_count_out=row_count_out,
    rejected_records=invalid_count,
    key_columns_null_pct=null_pct,
    schema_drift_flag=schema_drift_flag,
    execution_time_sec=round(execution_time, 2),
    status="WARN" if (invalid_count > 0 or schema_drift_flag) else "PASS",
    details=f"Silver unificada: chicago={chicago_count_in}, london={london_count_in}, nyc={nyc_count_in}, berlin={berlin_count_in}",
)

print(f"Silver processada: {row_count_out} registros.")
