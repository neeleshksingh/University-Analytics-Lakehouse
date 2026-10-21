from pyspark.sql import Window
from pyspark.sql import functions as F

from src.lakehouse.config import BRONZE, SILVER
from src.lakehouse.spark import get_spark
from src.source_db.schema import SOURCE_TABLES

from delta.tables import DeltaTable
from src.lakehouse.config import OPS


AUDIT_COLUMNS = ["_batch_id", "_ingest_date", "_ingested_at", "_source_file"]

STANDARDIZE = {
    "student": {"city": F.initcap, "state": F.initcap, "email": F.lower},
    "faculty": {"email": F.lower},
}

PROGRESS = f"{OPS}/silver_progress"

def standardize(df, table_name):
    for column, rule in STANDARDIZE.get(table_name, {}).items():
        df = df.withColumn(column, rule(F.col(column)))
        pass
    return df

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


def processed_batches(spark, table_name):
    """Bronze batches Silver has already merged for this table."""
    if not DeltaTable.isDeltaTable(spark, PROGRESS):
        return set()
    rows = (spark.read.format("delta").load(PROGRESS)
            .filter(F.col("table_name") == table_name)
            .select("batch_id").collect())
    return {row["batch_id"] for row in rows}


def record_batches(spark, table_name, batch_ids):
    """Remember these batches as done. Called only after a successful MERGE."""
    rows = [(str(table_name), str(b)) for b in batch_ids]
    (spark.createDataFrame(rows, "table_name string, batch_id string")
        .withColumn("processed_at", F.current_timestamp())
        .write.format("delta").mode("append").save(PROGRESS))


def prepare(df, key, table_name):
    """Turn raw Bronze rows into clean Silver rows (one per key)."""
    df = latest_rows(df, key)
    df = clean_text(df)
    df = standardize(df, table_name)
    df = df.drop(*AUDIT_COLUMNS)
    return df.withColumn("_silver_updated_at", F.current_timestamp())


def load_table(spark, table):
    key = list(table.primary_key.columns)[0].name
    path = f"{SILVER}/{table.name}"
    bronze = spark.read.format("delta").load(f"{BRONZE}/{table.name}")

    done = processed_batches(spark, table.name)
    new_batches = [r["_batch_id"] for r in bronze.select("_batch_id").distinct().collect()
                   if r["_batch_id"] not in done]
    if not new_batches:
        return 0

    changes = prepare(bronze.filter(F.col("_batch_id").isin(new_batches)), key, table.name)

    if not DeltaTable.isDeltaTable(spark, path):
        # First run: the table doesn't exist yet, so create it
        changes.write.format("delta").save(path)
    else:
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("t")
            .merge(changes.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute()
        )
        pass

    record_batches(spark, table.name, new_batches)
    return changes.count()


def main():
    spark = get_spark("silver")
    for table in SOURCE_TABLES:
        n = load_table(spark, table)
        print(f"{table.name:<22} {n:>12,} rows merged")
    spark.stop()


if __name__ == "__main__":
    main()