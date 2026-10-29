from pyspark.sql import functions as F

from src.lakehouse.config import SILVER, GOLD
from src.lakehouse.spark import get_spark
from delta.tables import DeltaTable   # add to imports

TRACKED = ["program_id", "status", "city", "state"]   # changes here create a new version
OPEN_END = "9999-12-31"
STUDENT_COLUMNS = ["student_sk", "student_id", "roll_no", "full_name", "gender", "admission_year",
                   "program_id", "city", "state", "status",
                   "valid_from", "valid_to", "is_current", "row_hash"]

def silver(spark, name):
    """Read a Silver table, without soft-deleted rows."""
    df = spark.read.format("delta").load(f"{SILVER}/{name}")
    return df.filter(~F.col("is_deleted"))


def save(df, name):
    """Gold tables are rebuilt from Silver, so overwrite (including schema changes)."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").save(f"{GOLD}/{name}"))

def departments(spark):
    """Department columns, renamed so they don't clash with the table we join to."""
    return silver(spark, "department").select(
        "department_id",
        F.col("dept_code").alias("department_code"),
        F.col("name").alias("department_name"),
    )

def build_dim_date(spark):
    """One row per calendar day, from the first academic year to today."""
    bounds = silver(spark, "academic_year").agg(
        F.min("start_date").alias("lo"),
        F.greatest(F.max("end_date"), F.current_date()).alias("hi"),
    )
    dates = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),   # 2024-07-03 -> 20240703
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "E").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_program(spark):
    """Program with its department flattened in (star, not snowflake)."""
    df = silver(spark, "program").join(departments(spark), on="department_id", how="inner")
    return df.select(
        "program_id", "program_code", F.col("name").alias("program_name"),
        "degree_level", "duration_terms",
        "department_code", "department_name",
    )


def build_dim_course(spark):
    df = silver(spark, "course").join(departments(spark), on="department_id", how="inner")
    return df.select(
        "course_id", "course_code", F.col("title").alias("course_title"),
        "credits", "course_type", "max_marks",
        "department_code", "department_name",
    )

def build_dim_faculty(spark):
    df = silver(spark, "faculty").join(departments(spark), on="department_id", how="inner")
    return df.select(
        "faculty_id", "faculty_code", F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "designation", "status", "joined_on",
        "department_code", "department_name"
    )


def build_dim_term(spark):
    years = silver(spark, "academic_year").select(
        "academic_year_id",                          
        F.col("label").alias("academic_year"),      
    )
    df = silver(spark, "term").join(years, on="academic_year_id", how="inner")
    return df.select(
        "term_id", "term_no", F.col("name").alias("term_name"),
        "start_date", "end_date", "academic_year",
    )


DIMENSIONS = {
    "dim_date": build_dim_date,
    "dim_program": build_dim_program,
    "dim_faculty": build_dim_faculty,
    "dim_course": build_dim_course,
    "dim_term": build_dim_term
}

def student_snapshot(spark):
    """Current Silver students + a fingerprint of the tracked columns."""
    fingerprint = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in TRACKED])
    return silver(spark, "student").select(
        "student_id", "roll_no",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "gender", "admission_year", "program_id", "city", "state", "status",
        F.col("updated_at").alias("valid_from"),
        F.sha2(fingerprint, 256).alias("row_hash"),
    )


def with_scd_columns(df):
    """Surrogate key + open-ended validity."""
    return (df
        .withColumn("student_sk", F.xxhash64("student_id", "valid_from"))
        .withColumn("valid_to", F.lit(OPEN_END).cast("timestamp"))
        .withColumn("is_current", F.lit(True)))


def build_dim_student(spark):
    path = f"{GOLD}/dim_student"
    incoming = student_snapshot(spark)

    if not DeltaTable.isDeltaTable(spark, path):
        first = incoming.withColumn("valid_from", F.lit("1900-01-01").cast("timestamp"))
        with_scd_columns(first).select(STUDENT_COLUMNS).write.format("delta").save(path)
        return

    current = (spark.read.format("delta").load(path)
               .filter("is_current")
               .select("student_id", F.col("row_hash").alias("old_hash")))

    changed = (current.join(incoming, on="student_id", how="inner")
               .filter(F.col("row_hash") != F.col("old_hash"))
               .drop("old_hash"))

    new = incoming.join(current, on="student_id", how="left_anti")

    no_key = F.lit(None).cast("int")
    staged = with_scd_columns(
        changed.withColumn("merge_key", F.col("student_id"))
        .unionByName(changed.withColumn("merge_key", no_key))
        .unionByName(new.withColumn("merge_key", no_key))   
    )

    (DeltaTable.forPath(spark, path).alias("t")
        .merge(staged.alias("s"), "t.student_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={
            "is_current": "false",
            "valid_to": "s.valid_from"
        })
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in STUDENT_COLUMNS})
        .execute())


def main():
    spark = get_spark("gold")
    for name, build in DIMENSIONS.items():
        df = build(spark)
        save(df, name)
        print(f"{name:<22} {spark.read.format('delta').load(f'{GOLD}/{name}').count():>10,} rows")
    build_dim_student(spark)
    dim = spark.read.format("delta").load(f"{GOLD}/dim_student")
    print(f"{'dim_student':<22} {dim.count():>10,} rows ({dim.filter('is_current').count():,} current)")
    spark.stop()


if __name__ == "__main__":
    main()