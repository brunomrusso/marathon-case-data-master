# Databricks notebook source

# MAGIC %md
# MAGIC # Weather Enrichment
# MAGIC Enriquece os dados das maratonas com as condições climáticas do dia da prova, usando a API pública Open-Meteo (sem API key).
# MAGIC
# MAGIC Camadas:
# MAGIC - Bronze: `bronze.marathon_metadata` (data/local das provas) e `bronze.weather_raw` (dados brutos da API).
# MAGIC - Silver: `silver.marathons_with_weather` (resultados + clima).
# MAGIC
# MAGIC A API retorna temperatura, precipitação e vento para o dia da prova. Rastreia run_id/batch_id e loga métricas de qualidade.

# COMMAND ----------

# MAGIC %pip install pyyaml requests

# COMMAND ----------

import calendar
import json
import sys
import time
import uuid
import yaml
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
)

# COMMAND ----------

spark = SparkSession.builder.appName("WeatherEnrichment").getOrCreate()
spark.conf.set("spark.sql.ansi.enabled", "false")

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
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


# COMMAND ----------

# Metadata: nome, cidade, país, coordenadas e regra para estimar a data da prova
MARATHON_METADATA = [
    {
        "source": "berlin",
        "marathon_name": "BMW Berlin Marathon",
        "city": "Berlin",
        "country": "DEU",
        "latitude": 52.5200,
        "longitude": 13.4050,
    },
    {
        "source": "chicago",
        "marathon_name": "Chicago Marathon",
        "city": "Chicago",
        "country": "USA",
        "latitude": 41.8781,
        "longitude": -87.6298,
    },
    {
        "source": "nyc",
        "marathon_name": "TCS New York City Marathon",
        "city": "New York",
        "country": "USA",
        "latitude": 40.7128,
        "longitude": -74.0060,
    },
    {
        "source": "london",
        "marathon_name": "TCS London Marathon",
        "city": "London",
        "country": "GBR",
        "latitude": 51.5074,
        "longitude": -0.1278,
    },
]


def nth_weekday_of_month(year, month, weekday, n):
    """Retorna a n-ésima ocorrência de um dia da semana no mês.
    weekday: 0=Segunda, 6=Domingo. n=-1 retorna a última ocorrência.
    """
    if n == -1:
        last_day = calendar.monthrange(year, month)[1]
        last_date = datetime(year, month, last_day)
        days_back = (last_date.weekday() - weekday) % 7
        return last_date - timedelta(days=days_back)
    first_day = datetime(year, month, 1)
    days_forward = (weekday - first_day.weekday()) % 7
    first_occurrence = first_day + timedelta(days=days_forward)
    return first_occurrence + timedelta(weeks=n - 1)


def estimate_race_date(source, year):
    """Estima a data da prova com base no padrão histórico de cada maratona."""
    if source == "berlin":
        # Último domingo de setembro
        return nth_weekday_of_month(year, 9, 6, -1)
    if source == "chicago":
        # Segundo domingo de outubro
        return nth_weekday_of_month(year, 10, 6, 2)
    if source == "nyc":
        # Primeiro domingo de novembro
        return nth_weekday_of_month(year, 11, 6, 1)
    if source == "london":
        # 2020 e 2021 foram em outubro por causa da COVID; demais anos: último domingo de abril
        if year in (2020, 2021):
            return nth_weekday_of_month(year, 10, 6, 1)
        return nth_weekday_of_month(year, 4, 6, -1)
    raise ValueError(f"Fonte desconhecida: {source}")


# COMMAND ----------

# Descobre os anos presentes na Silver
silver_df = spark.table("silver.marathons")
years_in_data = silver_df.select("source", "year").distinct().toPandas()

# Tenta carregar metadata exata de CSV em raw/marathon_metadata.csv; se não existir, usa heurística
metadata_csv_path = f"abfss://{container}@{storage}.dfs.core.windows.net/raw/marathon_metadata.csv"
metadata_df = None

try:
    candidate_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(metadata_csv_path)
        .select("source", "year", "marathon_name", "city", "country", "latitude", "longitude", "race_date")
        .filter(col("source").isin([m["source"] for m in MARATHON_METADATA])))
    if candidate_df.count() > 0:
        metadata_df = candidate_df
        print("Metadata carregada do CSV em raw/marathon_metadata.csv")
        metadata_df.show()
    else:
        print("CSV de metadata vazio; usando heurística.")
except Exception as e:
    print(f"CSV de metadata não encontrado; usando heurística. Detalhe: {e}")

if metadata_df is None:
    metadata_rows = []
    for _, row in years_in_data.iterrows():
        source = row["source"]
        year = row["year"]
        meta = next((m for m in MARATHON_METADATA if m["source"] == source), None)
        if not meta or year is None:
            continue
        race_date = estimate_race_date(source, int(year))
        metadata_rows.append(
            {
                "source": source,
                "year": int(year),
                "marathon_name": meta["marathon_name"],
                "city": meta["city"],
                "country": meta["country"],
                "latitude": meta["latitude"],
                "longitude": meta["longitude"],
                "race_date": race_date.strftime("%Y-%m-%d"),
            }
        )
    metadata_df = spark.createDataFrame(metadata_rows)
    print(f"Metadata gerada via heurística para {metadata_rows.__len__()} combinações source/ano.")

metadata_df.show()
metadata_count_in = metadata_df.count()

# Persiste a metadata como tabela Bronze (merge/upsert por source + year)
metadata_path = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/marathon_metadata"

try:
    spark.table("bronze.marathon_metadata")
    metadata_df.createOrReplaceTempView("metadata_staging")
    spark.sql("""
        MERGE INTO bronze.marathon_metadata AS target
        USING metadata_staging AS source
        ON target.source = source.source AND target.year = source.year
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Tabela bronze.marathon_metadata atualizada via MERGE.")
except Exception:
    try:
        dbutils.fs.rm(metadata_path, recurse=True)
    except Exception:
        pass
    metadata_df.write.format("delta").mode("overwrite").option("path", metadata_path).saveAsTable(
        "bronze.marathon_metadata"
    )
    print("Tabela bronze.marathon_metadata criada.")

# COMMAND ----------

# Tabela cache de clima na Bronze (idempotente: não re-consulta APIs para datas já buscadas)
cache_path = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/weather_raw"

try:
    existing_weather = spark.table("bronze.weather_raw")
    existing_keys = existing_weather.select("source", "year", "race_date").distinct()
    metadata_to_fetch = metadata_df.join(existing_keys, ["source", "year", "race_date"], "leftanti")
    print("Usando cache existente; buscando apenas registros novos.")
except Exception:
    metadata_to_fetch = metadata_df
    print("Nenhum cache existente; buscando todos os registros.")

# COMMAND ----------

# Busca na Open-Meteo Archive API


def fetch_weather(latitude, longitude, race_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": race_date,
        "end_date": race_date,
        "daily": (
            "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
            "apparent_temperature_max,precipitation_sum,windspeed_10m_max"
        ),
        "timezone": "auto",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        if not daily or not daily.get("time"):
            return None
        return {
            "temperature_max_c": float(daily["temperature_2m_max"][0]) if daily["temperature_2m_max"][0] is not None else None,
            "temperature_min_c": float(daily["temperature_2m_min"][0]) if daily["temperature_2m_min"][0] is not None else None,
            "temperature_mean_c": float(daily["temperature_2m_mean"][0]) if daily["temperature_2m_mean"][0] is not None else None,
            "apparent_temperature_max_c": float(daily["apparent_temperature_max"][0]) if daily["apparent_temperature_max"][0] is not None else None,
            "precipitation_mm": float(daily["precipitation_sum"][0]) if daily["precipitation_sum"][0] is not None else None,
            "windspeed_max_kmh": float(daily["windspeed_10m_max"][0]) if daily["windspeed_10m_max"][0] is not None else None,
        }
    except Exception as e:
        print(f"Erro ao buscar clima para {latitude},{longitude} em {race_date}: {e}")
        return None


fetch_rows = metadata_to_fetch.toPandas()
weather_results = []
api_failures = 0

for _, row in fetch_rows.iterrows():
    race_date = row["race_date"]
    if not race_date:
        api_failures += 1
        continue
    weather = fetch_weather(row["latitude"], row["longitude"], race_date)
    if weather is None:
        api_failures += 1
        continue
    weather_results.append(
        {
            "source": row["source"],
            "year": int(row["year"]),
            "marathon_name": row["marathon_name"],
            "city": row["city"],
            "country": row["country"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "race_date": race_date,
            **weather,
            "api_response_timestamp": datetime.utcnow().isoformat(),
            "ingestion_date": datetime.utcnow().strftime("%Y-%m-%d"),
        }
    )

print(f"{len(weather_results)} registros de clima obtidos; {api_failures} falhas.")

# COMMAND ----------

# Grava cache de clima (merge/upsert por source + year + race_date)

weather_count_out = len(weather_results)

if weather_results:
    new_weather_df = spark.createDataFrame(weather_results)

    try:
        spark.table("bronze.weather_raw")
        new_weather_df.createOrReplaceTempView("weather_staging")
        spark.sql("""
            MERGE INTO bronze.weather_raw AS target
            USING weather_staging AS source
            ON target.source = source.source
               AND target.year = source.year
               AND target.race_date = source.race_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("Tabela bronze.weather_raw atualizada via MERGE.")
    except Exception:
        try:
            dbutils.fs.rm(cache_path, recurse=True)
        except Exception:
            pass
        new_weather_df.write.format("delta").mode("overwrite").option("path", cache_path).saveAsTable(
            "bronze.weather_raw"
        )
        print("Tabela bronze.weather_raw criada.")
else:
    print("Nenhum dado novo de clima para buscar.")

# COMMAND ----------

weather_df = spark.table("bronze.weather_raw")

weather_cols = [
    "source",
    "year",
    "race_date",
    "temperature_max_c",
    "temperature_min_c",
    "temperature_mean_c",
    "apparent_temperature_max_c",
    "precipitation_mm",
    "windspeed_max_kmh",
]

silver_with_weather = silver_df.join(
    weather_df.select(*weather_cols),
    on=["source", "year"],
    how="left",
)

silver_with_weather_count = silver_with_weather.count()

silver_with_weather_path = (
    f"abfss://{container}@{storage}.dfs.core.windows.net/silver/marathons_with_weather"
)

try:
    dbutils.fs.rm(silver_with_weather_path, recurse=True)
except Exception:
    pass

silver_with_weather.write.format("delta").mode("overwrite").option(
    "path", silver_with_weather_path
).saveAsTable("silver.marathons_with_weather")

print("Tabela silver.marathons_with_weather criada/atualizada.")

# COMMAND ----------

# % de nulos em colunas de clima para detectar falhas de enriquecimento
weather_key_cols = ["temperature_max_c", "temperature_mean_c", "precipitation_mm", "windspeed_max_kmh"]
weather_null_pct = {}
for c in weather_key_cols:
    if c in silver_with_weather.columns:
        total = silver_with_weather_count
        null_count = silver_with_weather.filter(col(c).isNull()).count() if total > 0 else 0
        weather_null_pct[c] = round(null_count / total * 100, 2) if total > 0 else 0.0

execution_time = time.time() - start_time

log_data_quality(
    layer="weather",
    step="weather_enrichment",
    row_count_in=metadata_count_in,
    row_count_out=weather_count_out,
    rejected_records=api_failures,
    key_columns_null_pct=weather_null_pct,
    schema_drift_flag=False,
    execution_time_sec=round(execution_time, 2),
    status="WARN" if api_failures > 0 else "PASS",
    details=f"Silver rows enriquecidos: {silver_with_weather_count}; weather_null_pct: {weather_null_pct}",
)

# COMMAND ----------
