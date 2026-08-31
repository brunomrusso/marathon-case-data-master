from pyspark.sql import DataFrame


def check_not_null(df: DataFrame, columns: list) -> DataFrame:
    total = df.count()
    for col_name in columns:
        nulls = df.filter(df[col_name].isNull()).count()
        if nulls > 0:
            raise ValueError(f"Coluna {col_name} possui {nulls} valores nulos de {total}")
    return df


def check_unique(df: DataFrame, columns: list) -> int:
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    return total - distinct
