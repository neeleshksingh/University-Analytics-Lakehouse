import uuid

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.lakehouse import storage
from src.lakehouse.dq import apply_rules, critical_checks, CriticalCheckFailed
from src.lakehouse.spark import get_spark
from src.source_db.schema import SOURCE_TABLES, FULL_LOAD_TABLES

AUDIT_COLUMNS = ["_batch_id", "_ingest_date", "_ingested_at", "_source_file"]

STANDARDIZE = {
    "student": {"city": F.initcap, "state": F.initcap, "email": F.lower},
    "faculty": {"email": F.lower},
}


def standardize(df, table_name):
    for column, rule in STANDARDIZE.get(table_name, {}).items():
        df = df.withColumn(column, rule(F.col(column)))
    return df


def latest_rows(df, key):
    """Keep only the newest version of each record."""
    newest_first = Window.partitionBy(key).orderBy(
        F.col("updated_at").desc(), F.col("_ingested_at").desc()
    )
    df = df.withColumn("_rn", F.row_number().over(newest_first))
    df = df.filter(F.col("_rn") == 1)
    return df.drop("_rn")


def clean_text(df):
    """Trim spaces from every text column: '  ranchi ' -> 'ranchi'."""
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def processed_batches(spark, table_name):
    """Bronze batches Silver has already merged for this table."""
    if not storage.exists(spark, "ops", "silver_progress"):
        return set()
    rows = (storage.read(spark, "ops", "silver_progress")
            .filter(F.col("table_name") == table_name)
            .select("batch_id").collect())
    return {row["batch_id"] for row in rows}


def record_batches(spark, table_name, batch_ids):
    """Remember these batches as done. Called only after a successful MERGE."""
    rows = [(str(table_name), str(b)) for b in batch_ids]
    progress = (spark.createDataFrame(rows, "table_name string, batch_id string")
                .withColumn("processed_at", F.current_timestamp()))
    storage.write(progress, "ops", "silver_progress")


def prepare(df, key, table_name):
    """Turn raw Bronze rows into clean Silver rows (one per key)."""
    df = latest_rows(df, key)
    df = clean_text(df)
    df = standardize(df, table_name)
    df = df.drop(*AUDIT_COLUMNS)
    return df.withColumn("_silver_updated_at", F.current_timestamp())


def add_registration_flag(spark, df):
    """Mark each attendance row: is the student registered in this class's course offering?"""
    sessions = storage.read(spark, "silver", "class_session").select("session_id", "offering_id")
    registrations = (
        storage.read(spark, "silver", "course_registration")
        .select("student_id", "offering_id").distinct()
        .withColumn("_dq_is_registered", F.lit(True))
    )
    df = df.join(sessions, on="session_id", how="left")
    df = df.join(registrations, on=["student_id", "offering_id"], how="left")
    df = df.withColumn("_dq_is_registered", F.coalesce(F.col("_dq_is_registered"), F.lit(False)))
    return df.drop("offering_id")


ENRICH = {"attendance": add_registration_flag}


def load_table(spark, table):
    name = str(table.name)
    key = str(list(table.primary_key.columns)[0].name)
    bronze = storage.read(spark, "bronze", name)

    done = processed_batches(spark, name)
    new_batches = [r["_batch_id"] for r in bronze.select("_batch_id").distinct().collect()
                   if r["_batch_id"] not in done]
    if not new_batches:
        return 0

    changes = prepare(bronze.filter(F.col("_batch_id").isin(new_batches)), key, name)

    enrich = ENRICH.get(name)
    if enrich:
        changes = enrich(spark, changes)
    changes = changes.cache()

    # Data quality: critical checks stop the run, row rules quarantine
    run_id = uuid.uuid4().hex[:12]

    previous = None
    if name in FULL_LOAD_TABLES and storage.exists(spark, "silver", name):
        previous = storage.read(spark, "silver", name).count()

    critical_results, failures = critical_checks(changes, name, key, run_id, previous)
    changes, quarantine, results = apply_rules(changes, name, key, run_id)
    results = critical_results + results

    changes = changes.drop(*[c for c in changes.columns if c.startswith("_dq_")])

    if quarantine is not None:
        storage.write(quarantine, "ops", "quarantine")
    if results:
        scorecard = (spark.createDataFrame(results, "run_id string, table_name string, rule string, "
                                                    "severity string, rows_checked long, rows_failed long")
                     .withColumn("checked_at", F.current_timestamp()))
        storage.write(scorecard, "ops", "dq_results")

    if failures:
        raise CriticalCheckFailed("; ".join(failures))

    if not storage.exists(spark, "silver", name):
        storage.write(changes, "silver", name)
    else:
        (storage.delta_table(spark, "silver", name).alias("t")
            .merge(changes.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute())

    record_batches(spark, name, new_batches)
    n = changes.count()
    changes.unpersist()
    return n


def main():
    spark = get_spark("silver")
    tables = sorted(SOURCE_TABLES, key=lambda t: str(t.name) in ENRICH)
    for table in tables:
        n = load_table(spark, table)
        print(f"{table.name:<22} {n:>12,} rows processed")
    spark.stop()


if __name__ == "__main__":
    main()