# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Ingestão com Auto Loader
# MAGIC Ingestão incremental de arquivos CSV via Databricks Auto Loader (`cloudFiles`).
# MAGIC Cada fonte possui sua própria subpasta em `raw/` (`raw/berlin/`, `raw/chicago/`,
# MAGIC `raw/london/`, `raw/nyc/`), seu próprio schema inferido e seu checkpoint.
# MAGIC O workflow continua disparado por **File Arrival Trigger**; dentro do job,
# MAGIC o Auto Loader processa apenas arquivos ainda não vistos e aplica MERGE
# MAGIC idempotente na camada Bronze.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import json
import re
import sys
import time
import uuid
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, input_file_name, regexp_extract
)
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

sys.path.insert(0, "../src")

# COMMAND ----------

dbutils.widgets.text("raw_dir", "", "raw_dir")
dbutils.widgets.text("run_id", "manual", "run_id")
dbutils.widgets.text("batch_id", "manual", "batch_id")

# COMMAND ----------

config_yaml = dbutils.secrets.get("marathon-scope", "config_yaml")
config = yaml.safe_load(config_yaml)
storage = config["azure"]["storage_account"]
container = config["azure"]["container"]

catalog_name = dbutils.secrets.get("marathon-scope", "catalog_name")
spark = SparkSession.builder.appName("BronzeAutoLoader").getOrCreate()
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

raw_dir = dbutils.widgets.get("raw_dir")
if not raw_dir:
    raw_dir = f"abfss://{container}@{storage}.dfs.core.windows.net/raw"
raw_dir = raw_dir.rstrip("/")

run_id = dbutils.widgets.get("run_id")
if run_id == "manual":
    run_id = str(uuid.uuid4())
batch_id = dbutils.widgets.get("batch_id")
if batch_id == "manual":
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
    print(f"run_id={run_id} / batch_id={batch_id} propagados via taskValues.")
except Exception as e:
    print(f"taskValues indisponivel (execucao manual?): {e}")

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


SOURCE_CONFIGS = {
    "berlin": {"delimiter": ";"},
    "chicago": {"delimiter": ";"},
    "london": {"delimiter": ","},
    "nyc": {"delimiter": ","},
}


def sanitize(name):
    name = re.sub(r"[ ,;{}()\n\t=]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    return name if name else "_col"


class BronzeBatchProcessor:
    """Callable serializavel para foreachBatch do Auto Loader.

    Evita problemas de closure com funcoes/lambdas capturando variaveis de
    loop; os parametros necessarios ficam como atributos da instancia.
    """

    def __init__(self, source, config, storage, container):
        self.source = source
        self.config = config
        self.storage = storage
        self.container = container

    def __call__(self, batch_df, batch_id):
        import traceback
        from delta.tables import DeltaTable

        try:
            if batch_df.isEmpty():
                print(f"  [{self.source}] batch {batch_id} vazio")
                return

            new_cols = [sanitize(c) for c in batch_df.columns]
            df = batch_df.toDF(*new_cols)

            # Extrai o ano: prefere coluna 'year' do CSV; senao extrai do path
            year_col = next((c for c in df.columns if c.lower() == "year"), None)
            if year_col is None:
                df = df.withColumn("year", regexp_extract(input_file_name(), r"(\d{4})", 1).cast("int"))
            else:
                if year_col != "year":
                    df = df.withColumnRenamed(year_col, "year")
                df = df.withColumn("year", col("year").cast("int"))

            if df.filter(col("year").isNull()).count() > 0:
                raise ValueError(f"Ano nulo detectado em {self.source}. Verifique se o CSV possui coluna 'year' ou se o nome do arquivo contem um ano (YYYY).")

            # Colunas que compoem o hash (exclui metadata adicionada depois)
            hash_cols = [c for c in df.columns if c not in ("source", "ingestion_date", "row_hash", "file_name")]

            df = (df
                  .withColumn("source", lit(self.source))
                  .withColumn("ingestion_date", current_timestamp())
                  .withColumn("row_hash", sha2(concat_ws("||", *hash_cols), 256))
                  .withColumn("file_name", input_file_name()))

            bronze_table = f"bronze.{self.source}"
            bronze_path = f"abfss://{self.container}@{self.storage}.dfs.core.windows.net/bronze/{self.source}"

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

            # Metadados de arquivo processado
            file_meta_df = (df.groupBy("year", "file_name")
                             .count()
                             .withColumn("source", lit(self.source))
                             .withColumn("ingestion_date", current_timestamp())
                             .withColumn("year", col("year").cast("int"))
                             .withColumn("rows", col("count").cast("long"))
                             .select("source", "year", "file_name", "rows", "ingestion_date"))
            if not spark.catalog.tableExists("bronze.file_metadata"):
                file_meta_df.write.format("delta").mode("overwrite").saveAsTable("bronze.file_metadata")
            else:
                file_meta_df.write.format("delta").mode("append").saveAsTable("bronze.file_metadata")

            print(f"  [{self.source}] batch {batch_id} processado: {df.count()} registros")
        except Exception as e:
            msg = f"[{self.source}] ERRO no foreachBatch: {type(e).__name__}: {e}\n{traceback.format_exc()}"
            print(msg)
            raise


# COMMAND ----------


processed_sources = []
for source, config in SOURCE_CONFIGS.items():
    src_dir = f"{raw_dir}/{source}"
    try:
        dbutils.fs.ls(src_dir)
    except Exception:
        print(f"Pasta {src_dir} nao encontrada; ignorando {source} nesta execucao.")
        continue

    schema_location = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/_schemas/{source}"
    checkpoint_location = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze/_checkpoints/{source}"

    print(f"Auto Loader: {source} <- {src_dir} (delimiter='{config['delimiter']}')")

    stream_df = (spark.readStream
                   .format("cloudFiles")
                   .option("cloudFiles.format", "csv")
                   .option("cloudFiles.schemaLocation", schema_location)
                   .option("cloudFiles.inferColumnTypes", "true")
                   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                   .option("cloudFiles.allowOverwrites", "true")
                   .option("header", "true")
                   .option("delimiter", config["delimiter"])
                   .load(src_dir))

    processor = BronzeBatchProcessor(source, config, storage, container)

    query = (stream_df.writeStream
             .foreachBatch(processor)
             .option("checkpointLocation", checkpoint_location)
             .queryName(f"bronze_{source}_autoloader")
             .trigger(availableNow=True)
             .start())

    processed_sources.append((source, query))

# COMMAND ----------

if not processed_sources:
    raise FileNotFoundError(f"Nenhuma subpasta de fonte encontrada em {raw_dir}/. Suba os CSVs nas pastas berlin/, chicago/, london/ e/ou nyc/.")

for source, query in processed_sources:
    print(f"Aguardando conclusao do stream {source}...")
    query.awaitTermination()

# COMMAND ----------

execution_time = time.time() - start_time

log_data_quality(
    layer="bronze",
    step="bronze_orchestrator",
    source="all",
    row_count_in=len(processed_sources),
    row_count_out=len(processed_sources),
    execution_time_sec=round(execution_time, 2),
    details=f"Fontes processadas via Auto Loader: {[s for s, _ in processed_sources]}",
)

print(f"Auto Loader concluido. Fontes processadas: {[s for s, _ in processed_sources]}")
