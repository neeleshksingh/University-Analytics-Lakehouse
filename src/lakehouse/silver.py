"""Silver: one clean, current row per record, built from Bronze."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.lakehouse.config import BRONZE, SILVER
from src.lakehouse.spark import get_spark
from src.source_db.schema import SOURCE_TABLES

AUDIT_COLUMNS = ["_batch_id", "_ingest_date", "_ingested_at", "_source_file"]


def latest_rows(df, key):
    """Keep only the newest version of each record."""
    newest_first = Window.partitionBy(key).orderBy(
        F.col("updated_at").desc(), F.col("_ingested_at").desc()
    )
    df = df.withColumn("_rn", F.row_number().over(newest_first))
    df = df.filter(F.col("_rn") == 1)
    df = df.drop("_rn")
    return df


def clean_text(df):
    """Trim spaces from every text column: '  ranchi ' -> 'ranchi'."""
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def build_table(spark, table):
    key = list(table.primary_key.columns)[0].name
    bronze = spark.read.format("delta").load(f"{BRONZE}/{table.name}")

    silver = latest_rows(bronze, key)
    silver = clean_text(silver)
    silver = silver.drop(*AUDIT_COLUMNS)

    _silver_updated_at = F.current_timestamp()
    silver = silver.withColumn("_silver_updated_at", _silver_updated_at)

    path = f"{SILVER}/{table.name}"
    silver.write.format("delta").mode("overwrite").save(path)
    return spark.read.format("delta").load(path).count()


def main():
    spark = get_spark("silver")
    for table in SOURCE_TABLES:
        print(f"{table.name:<22} {build_table(spark, table):>12,} rows")
    spark.stop()


if __name__ == "__main__":
    main()