"""Spark session: build one locally, reuse the platform's on Databricks."""
from pyspark.sql import SparkSession

from src.lakehouse import storage

TIMEZONE = "Asia/Kolkata"


def get_spark(app_name="lakehouse"):
    if storage.on_databricks():
        spark = SparkSession.builder.getOrCreate()            # Databricks already has a session
        spark.conf.set("spark.sql.session.timeZone", TIMEZONE)
        return spark

    from delta import configure_spark_with_delta_pip          # only needed locally

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark(spark):
    """Stop our own local session; never the platform's."""
    if not storage.on_databricks():
        spark.stop()