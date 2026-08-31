# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — ETL
# MAGIC Limpeza, padronização de schema, integração das fontes e mascaramento/anonimização.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, trim, regexp_extract, coalesce, sha2, concat_ws,
    concat, floor
)
from pyspark.sql.types import LongType

# COMMAND ----------

sys.path.insert(0, "../src")

# COMMAND ----------

spark = SparkSession.builder.appName("SilverETL").getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

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

def parse_time_to_seconds(time_col, output_name):
    return (when(col(time_col).isNotNull(),
                 (regexp_extract(col(time_col), r"(\d+):(\d+):(\d+)", 1).cast("int") * 3600 +
                  regexp_extract(col(time_col), r"(\d+):(\d+):(\d+)", 2).cast("int") * 60 +
                  regexp_extract(col(time_col), r"(\d+):(\d+):(\d+)", 3).cast("int"))
        ).otherwise(None)
        .cast(LongType())
        .alias(output_name))

# COMMAND ----------

# Chicago
chicago = spark.table("bronze.chicago")
chicago = (chicago
    .withColumn("source", lit("chicago"))
    .withColumn("marathon_name", lit("Chicago Marathon"))
    .withColumn("athlete_name", safe_get(chicago, "athlete_name", "athlete_name"))
    .withColumn("athlete_id", safe_get(chicago, "athlete_id", "athlete_id"))
    .withColumn("gender", normalize_gender("gender"))
    .withColumn("age_group", upper(trim(safe_get(chicago, "age_group", "age_group"))))
    .withColumn("country", upper(trim(safe_get(chicago, "country_ioc", "country"))))
    .withColumn("place_overall", safe_get(chicago, "place_overall", "place_overall").cast("int"))
    .withColumn("place_gender", safe_get(chicago, "place_gender", "place_gender").cast("int"))
    .withColumn("finish_time", safe_get(chicago, "finish_time", "finish_time"))
    .withColumn("half_time", lit(None).cast("string"))
    .withColumn("club", lit(None).cast("string"))
)
chicago = (chicago
    .withColumn("finish_time_sec", coalesce(
        col("finish_time_seconds").cast(LongType()),
        parse_time_to_seconds("finish_time", "finish_time_sec")))
    .withColumn("half_time_sec", coalesce(
        col("half_split_seconds").cast(LongType()),
        parse_time_to_seconds("half_time", "half_time_sec"))))

# London
london = spark.table("bronze.london")
london = (london
    .withColumn("source", lit("london"))
    .withColumn("marathon_name", lit("London Marathon"))
    .withColumn("athlete_name", regexp_extract(trim(col("Name")), r"^(.+?) \(", 1))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("country", upper(regexp_extract(trim(col("Name")), r"\((\w{3})\)", 1)))
    .withColumn("gender", normalize_gender("Gender"))
    .withColumn("age_group", upper(trim(safe_get(london, "Category", "age_group"))))
    .withColumn("place_overall", safe_get(london, "Overall Place", "place_overall").cast("int"))
    .withColumn("place_gender", safe_get(london, "Gender Place", "place_gender").cast("int"))
    .withColumn("finish_time", safe_get(london, "Finish Time", "finish_time"))
    .withColumn("half_time", safe_get(london, "Half Time", "half_time"))
    .withColumn("club", safe_get(london, "Club", "club"))
)
london = (london
    .withColumn("finish_time_sec", parse_time_to_seconds("finish_time", "finish_time_sec"))
    .withColumn("half_time_sec", parse_time_to_seconds("half_time", "half_time_sec")))

# New York
nyc = spark.table("bronze.nyc")
nyc = (nyc
    .withColumn("source", lit("nyc"))
    .withColumn("marathon_name", lit("New York City Marathon"))
    .withColumn("athlete_name", safe_get(nyc, "Name", "athlete_name"))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("gender", normalize_gender("Gender"))
    .withColumn("age_group", when(col("Age").isNotNull(), concat(
        (floor(col("Age") / 5) * 5).cast("int"),
        lit("-"),
        (floor(col("Age") / 5) * 5 + 4).cast("int"))
        ).otherwise(None))
    .withColumn("country", upper(trim(safe_get(nyc, "Country", "country"))))
    .withColumn("place_overall", safe_get(nyc, "Overall", "place_overall").cast("int"))
    .withColumn("place_gender", lit(None).cast("int"))
    .withColumn("finish_time", safe_get(nyc, "Finish Time", "finish_time"))
    .withColumn("half_time", lit(None).cast("string"))
    .withColumn("club", lit(None).cast("string"))
)
nyc = (nyc
    .withColumn("finish_time_sec", coalesce(
        col("Finish").cast(LongType()),
        parse_time_to_seconds("finish_time", "finish_time_sec")))
    .withColumn("half_time_sec", lit(None).cast(LongType())))

# Berlin
berlin = spark.table("bronze.berlin")
berlin = (berlin
    .withColumn("source", lit("berlin"))
    .withColumn("marathon_name", lit("Berlin Marathon"))
    .withColumn("athlete_name", safe_get(berlin, "name", "athlete_name"))
    .withColumn("athlete_id", lit(None).cast("string"))
    .withColumn("gender", normalize_gender("sex"))
    .withColumn("age_int", safe_get(berlin, "age_category", "age_category").cast("int"))
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

# COMMAND ----------

# Mascaramento/anonimização
union_df = (union_df
    .withColumn("athlete_id_hash", sha2(concat_ws("||", "source", "year",
        coalesce(col("athlete_id"), col("athlete_name"), col("place_overall").cast("string"))), 256))
    .drop("athlete_name")
    .drop("athlete_id"))

# COMMAND ----------

# Validações básicas
invalid = union_df.filter(col("gender").isNull() | (col("finish_time_sec") < 0))
if invalid.head(1):
    raise ValueError("Encontrados registros inválidos na Silver")

# COMMAND ----------

(union_df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .option("mergeSchema", "true")
 .saveAsTable("silver.marathons"))

print(f"Silver processada: {union_df.count()} registros.")
