# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Agregações
# MAGIC Geração das tabelas Gold para alimentar o dashboard final.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min, max, sum, round, when, expr
)

# COMMAND ----------

spark = SparkSession.builder.appName("GoldAggregations").getOrCreate()

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

silver = spark.table("silver.marathons")

# COMMAND ----------

# 1. gold.kpi_summary
kpi_summary = (silver
    .groupBy("source", "year", "marathon_name")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec"),
        min("finish_time_sec").alias("record_time_sec"),
        sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_count"),
        sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_count")
    )
    .withColumn("female_pct", round(col("female_count") / col("total_athletes") * 100, 2)))

(kpi_summary.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .saveAsTable("gold.kpi_summary"))

# COMMAND ----------

# 2. gold.finishers_by_year
finishers_by_year = (silver
    .groupBy("source", "year", "marathon_name")
    .agg(
        count("*").alias("total_finishers"),
        sum(when(col("gender") == "F", 1).otherwise(0)).alias("female_finishers"),
        sum(when(col("gender") == "M", 1).otherwise(0)).alias("male_finishers")
    )
    .withColumn("female_pct", round(col("female_finishers") / col("total_finishers") * 100, 2)))

(finishers_by_year.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("gold.finishers_by_year"))

# COMMAND ----------

# 3. gold.top_countries
top_countries = (silver
    .filter(col("country").isNotNull())
    .groupBy("source", "year", "country")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

(top_countries.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .saveAsTable("gold.top_countries"))

# COMMAND ----------

# 4. gold.athletes_by_country
athletes_by_country = (silver
    .filter(col("country").isNotNull())
    .groupBy("country")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

(athletes_by_country.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("gold.athletes_by_country"))

# COMMAND ----------

# 5. gold.times_distribution
times_distribution = (silver
    .filter(col("finish_time_sec").isNotNull())
    .groupBy("source", "year", "gender", "age_group")
    .agg(
        min("finish_time_sec").alias("min"),
        max("finish_time_sec").alias("max"),
        avg("finish_time_sec").alias("mean"),
        expr("percentile_approx(finish_time_sec, 0.5)").alias("median"),
        expr("percentile_approx(finish_time_sec, 0.25)").alias("q1"),
        expr("percentile_approx(finish_time_sec, 0.75)").alias("q3")
    ))

(times_distribution.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .saveAsTable("gold.times_distribution"))

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

(marathon_comparison.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year")
 .saveAsTable("gold.marathon_comparison"))

# COMMAND ----------

# 7. gold.age_gender_profile
age_gender_profile = (silver
    .groupBy("source", "year", "age_group", "gender")
    .agg(
        count("*").alias("total_athletes"),
        avg("finish_time_sec").alias("avg_finish_time_sec")
    ))

(age_gender_profile.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("source", "year")
 .saveAsTable("gold.age_gender_profile"))

# COMMAND ----------

print("Tabelas Gold geradas com sucesso.")
