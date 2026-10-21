import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.lakehouse.config import BRONZE, LANDING
from src.lakehouse.spark import get_spark
from src.source_db.schema import SOURCE_TABLES


def loaded_batches(spark, path):
    """Batch IDs already in this Bronze table (an empty set if the table doesn't exist yet)."""
    if not DeltaTable.isDeltaTable(spark, path):
        return set()
    rows = spark.read.format("delta").load(path).select("_batch_id").distinct().collect()
    return {row["_batch_id"] for row in rows}


def load_table(spark, name):
    source = f"{LANDING}/{name}"
    target = f"{BRONZE}/{name}"
    if not os.path.exists(source):
        return 0

    landing = spark.read.parquet(source)
    done = loaded_batches(spark, target)

    new_rows = landing
    new_rows = new_rows.filter(~F.col("batch_id").isin(list(done)))

    new_rows = new_rows.withColumnRenamed("batch_id", "_batch_id")
    new_rows = new_rows.withColumnRenamed("ingest_date", "_ingest_date")

    new_rows = new_rows.withColumn("_ingested_at", F.current_timestamp())
    new_rows = new_rows.withColumn("_source_file", F.input_file_name())

    count = new_rows.count()
    if count == 0:
        return 0

    new_rows.write.format("delta").mode("append").save(target)

    return count


def main():
    spark = get_spark("bronze")
    for table in SOURCE_TABLES:
        n = load_table(spark, table.name)
        print(f"{table.name:<22} {n:>12,} new rows")
    spark.stop()


if __name__ == "__main__":
    main()