from pyspark.sql import functions as F

from src.lakehouse.config import SILVER, GOLD
from src.lakehouse.spark import get_spark


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


def main():
    spark = get_spark("gold")
    for name, build in DIMENSIONS.items():
        df = build(spark)
        save(df, name)
        print(f"{name:<22} {spark.read.format('delta').load(f'{GOLD}/{name}').count():>10,} rows")
    spark.stop()


if __name__ == "__main__":
    main()