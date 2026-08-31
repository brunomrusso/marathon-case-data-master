import yaml
from pyspark.sql import SparkSession


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_spark(app_name: str = "MarathonCase") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def build_adls_path(storage_account: str, container: str, layer: str, source: str, year: int) -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{layer}/{source}/{year}"
