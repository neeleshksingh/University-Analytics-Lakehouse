from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.lakehouse.config import SILVER, GOLD
from src.lakehouse.spark import get_spark

TRACKED = ["program_id", "status", "city", "state"]
OPEN_END = "9999-12-31"
STUDENT_COLUMNS = ["student_sk", "student_id", "roll_no", "full_name", "gender", "admission_year",
                   "program_id", "city", "state", "status",
                   "valid_from", "valid_to", "is_current", "row_hash"]


# ---------------------------------------------------------------- helpers

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


def date_key(col):
    """2024-07-03 -> 20240703, the key used by dim_date."""
    return F.date_format(col, "yyyyMMdd").cast("int")


# ---------------------------------------------------------------- dimensions

def build_dim_date(spark):
    """One row per calendar day, from the first academic year to today."""
    bounds = silver(spark, "academic_year").agg(
        F.min("start_date").alias("lo"),
        F.greatest(F.max("end_date"), F.current_date()).alias("hi"),
    )
    dates = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return dates.select(
        date_key("date").alias("date_key"),
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
        "faculty_id", "faculty_code",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "designation", "status", "joined_on",
        "department_code", "department_name",
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

def build_dim_fee_component(spark):
    return silver(spark, "fee_component").select(
        "fee_component_id", F.col("code").alias("fee_code"), F.col("name").alias("fee_name"), "is_refundable"
    )


DIMENSIONS = {
    "dim_date": build_dim_date,
    "dim_program": build_dim_program,
    "dim_faculty": build_dim_faculty,
    "dim_course": build_dim_course,
    "dim_term": build_dim_term,
    "dim_fee_component": build_dim_fee_component,
}


# ---------------------------------------------------------------- dim_student (SCD Type 2)

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
            "valid_to": "s.valid_from",
        })
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in STUDENT_COLUMNS})
        .execute())


def attach_student_sk(spark, df, at_col):
    """Add student_sk: the dim_student version valid at the timestamp in `at_col`.
    df must have student_id. Returns df plus student_sk."""
    students = (spark.read.format("delta").load(f"{GOLD}/dim_student")
                .select("student_sk",
                        F.col("student_id").alias("dim_student_id"),
                        "valid_from", "valid_to"))

    in_version = (
        (F.col("student_id") == F.col("dim_student_id"))
        & (F.col(at_col) >= F.col("valid_from"))
        & (F.col(at_col) < F.col("valid_to"))
    )

    return (df.join(F.broadcast(students), on=in_version, how="inner")
              .drop("dim_student_id", "valid_from", "valid_to"))


# ---------------------------------------------------------------- facts

def build_fact_attendance(spark):
    """Grain: one row per student per class session."""
    # attendance --session_id--> class_session --offering_id--> course_offering
    attendance = silver(spark, "attendance").select(
        "attendance_id", "session_id", "student_id", "status", "marked_at")
    sessions = silver(spark, "class_session").select("session_id", "offering_id", "session_date")
    offerings = silver(spark, "course_offering").select("offering_id", "course_id", "term_id", "faculty_id")

    df = (attendance
          .join(sessions, on="session_id", how="inner")
          .join(offerings, on="offering_id", how="inner"))
    df = attach_student_sk(spark, df, "marked_at")

    return df.select(
        "attendance_id",
        date_key("session_date").alias("date_key"),
        "student_sk", "student_id", "course_id", "term_id", "faculty_id", "offering_id", "session_id",
        "status",
        F.col("status").isin("present", "late").cast("int").alias("is_present"),
        (F.col("status") == "excused").cast("int").alias("is_excused"),
    )


def build_fact_exam_result(spark):
    """Grain: one row per student per exam per course."""
    # exam_result --registration_id--> course_registration --offering_id--> course_offering
    # exam_result --exam_id--> exam
    results = silver(spark, "exam_result").select(
        "result_id", "exam_id", "registration_id", "marks_obtained", "max_marks", "grade", "result_status")
    registrations = silver(spark, "course_registration").select("registration_id", "student_id", "offering_id")
    offerings = silver(spark, "course_offering").select("offering_id", "course_id", "term_id", "faculty_id")
    exams = silver(spark, "exam").select(
        "exam_id", "exam_type", F.col("end_date").cast("timestamp").alias("exam_at"))

    df = (results
          .join(registrations, on="registration_id", how="inner")
          .join(offerings, on="offering_id", how="inner")
          .join(exams, on="exam_id", how="inner"))
    df = attach_student_sk(spark, df, "exam_at")

    return df.select(
        "result_id",
        date_key("exam_at").alias("date_key"),
        "student_sk", "student_id", "course_id", "term_id", "faculty_id", "exam_id", "exam_type",
        "marks_obtained", "max_marks",
        F.round(F.col("marks_obtained") / F.col("max_marks") * 100, 2).alias("score_pct"),
        "grade", "result_status",
        (F.col("result_status") == "pass").cast("int").alias("is_pass"),
    )


def build_fact_payment(spark):
    """Grain: one row per payment attempt."""
    # payment --invoice_id--> fee_invoice --enrollment_id--> enrollment
    payments = silver(spark, "payment").select(
        "payment_id", "invoice_id", "amount", "mode", "status", "paid_at")
    invoices = silver(spark, "fee_invoice").select(
        "invoice_id", "enrollment_id", "fee_component_id", "due_date")   # no amount/status: they'd clash
    enrollments = silver(spark, "enrollment").select("enrollment_id", "student_id", "term_id")

    df = (payments
          .join(invoices, on="invoice_id", how="inner")
          .join(enrollments, on="enrollment_id", how="inner"))
    df = attach_student_sk(spark, df, "paid_at")

    return df.select(
        "payment_id",
        date_key("paid_at").alias("date_key"),
        "student_sk", "student_id", "term_id", "invoice_id", "fee_component_id",
        "mode", "status", "amount",
        F.when(F.col("status") == "success", F.col("amount")).otherwise(F.lit(0)).alias("amount_collected"),
        F.datediff(F.col("paid_at"), F.col("due_date")).alias("days_after_due"),
    )

def build_fact_fee_invoice(spark):
    """Grain: one row per invoice."""
    # fee_invoice --enrollment_id--> enrollment
    invoices = silver(spark, "fee_invoice").select(
        "invoice_id", "invoice_no", "enrollment_id", "fee_component_id",
        "issue_date", "due_date", "amount", "status",
        F.col("issue_date").cast("timestamp").alias("issued_at"),
    )
    enrollments = silver(spark, "enrollment").select("enrollment_id", "student_id", "term_id")

    df = invoices.join(enrollments, on="enrollment_id", how="inner")
    df = attach_student_sk(spark, df, "issued_at")

    return df.select(
        "invoice_id", "invoice_no",
        date_key("issue_date").alias("date_key"),
        "student_sk", "student_id", "term_id", "fee_component_id",
        "issue_date", "due_date",
        F.col("amount").alias("amount_due"),
        F.col("status").alias("invoice_status"),
    )


FACTS = {
    "fact_attendance": build_fact_attendance,
    "fact_exam_result": build_fact_exam_result,
    "fact_payment": build_fact_payment,
    "fact_fee_invoice": build_fact_fee_invoice,
}

# ---------------------------------------------------------------- marts

ELIGIBILITY_THRESHOLD = 75.0
EXCUSED_COUNTS_AS_PRESENT = True


def gold(spark, name):
    return spark.read.format("delta").load(f"{GOLD}/{name}")


def build_mart_attendance_eligibility(spark):
    """Grain: one row per student per course offering. Below threshold = not eligible for end-term."""
    attended = F.col("is_present") + (F.col("is_excused") if EXCUSED_COUNTS_AS_PRESENT else F.lit(0))

    per_course = (gold(spark, "fact_attendance")
                  .groupBy("student_id", "offering_id", "course_id", "term_id")
                  .agg(
                      F.count("*").alias("sessions_held"),
                      F.sum(attended).alias("sessions_attended"),
                      F.sum("is_excused").alias("sessions_excused"),
                  ))

    per_course = (per_course
        .withColumn("attendance_pct", F.round(F.col("sessions_attended") / F.col("sessions_held") * 100, 2))
        .withColumn("is_eligible", F.col("attendance_pct") >= ELIGIBILITY_THRESHOLD))

    students = (gold(spark, "dim_student").filter("is_current")
                .select("student_id", "roll_no", "full_name", "program_id", "status"))
    programs = gold(spark, "dim_program").select("program_id", "program_name")
    courses = gold(spark, "dim_course").select("course_id", "course_code", "course_title")
    terms = gold(spark, "dim_term").select("term_id", "term_name", "academic_year", "start_date", "end_date")

    df = (per_course
        .join(students, on="student_id", how="inner")
        .join(programs, on="program_id", how="inner")
        .join(courses, on="course_id", how="inner")
        .join(terms, on="term_id", how="inner"))

    df = (df
        .withColumn("is_term_complete", F.col("end_date") < F.current_date())
        .withColumn("eligibility_status",
            F.when(F.col("is_eligible"), "eligible")
             .when(F.col("is_term_complete"), "not_eligible")
             .otherwise("at_risk")))

    return df.select(
        "academic_year", "term_name", "start_date", "program_name", "course_code", "course_title",
        "roll_no", "full_name", F.col("status").alias("student_status"),
        "sessions_held", "sessions_attended", "sessions_excused",
        "attendance_pct", "is_eligible", "is_term_complete", "eligibility_status",
        "student_id", "offering_id", "course_id", "term_id",
    )

AGING_BUCKETS = [(0, "not_due"), (30, "1-30"), (60, "31-60"), (90, "61-90")]


def build_mart_fee_aging(spark):
    """Grain: one row per invoice with money still owed, aged as of the latest date in the data."""
    payments = gold(spark, "fact_payment")
    as_of = payments.agg(F.to_date(F.max("date_key").cast("string"), "yyyyMMdd")).first()[0]

    paid = payments.groupBy("invoice_id").agg(F.sum("amount_collected").alias("amount_paid"))
    invoices = gold(spark, "fact_fee_invoice").filter(F.col("invoice_status") != "cancelled")

    df = (invoices
          .join(paid, on="invoice_id", how="left")
          .withColumn("amount_paid", F.coalesce(F.col("amount_paid"), F.lit(0))))

    df = df.withColumn("outstanding", F.col("amount_due") - F.col("amount_paid")).filter(F.col("outstanding") > 0)

    df = df.withColumn("days_overdue", F.datediff(F.lit(as_of), "due_date"))

    bucket = F.lit("90+")
    for limit, label in reversed(AGING_BUCKETS):
        bucket = F.when(F.col("days_overdue") <= limit, label).otherwise(bucket)
    df = df.withColumn("aging_bucket", bucket).withColumn("as_of_date", F.lit(as_of))

    students = (gold(spark, "dim_student").filter("is_current")
                .select("student_id", "roll_no", "full_name", "program_id", "status"))
    programs = gold(spark, "dim_program").select("program_id", "program_name")
    terms = gold(spark, "dim_term").select("term_id", "term_name", "start_date")
    fees = gold(spark, "dim_fee_component").select("fee_component_id", "fee_name")

    df = (df
        .join(students, on="student_id", how="inner")
        .join(programs, on="program_id", how="inner")
        .join(terms, on="term_id", how="inner")
        .join(fees, on="fee_component_id", how="inner"))

    return df.select(
        "as_of_date", "term_name", "start_date", "program_name", "roll_no", "full_name",
        F.col("status").alias("student_status"), "fee_name", "invoice_no",
        "due_date", "amount_due", "amount_paid", "outstanding", "days_overdue", "aging_bucket",
        "invoice_id", "student_id", "term_id", "fee_component_id",
    )

def build_mart_course_results(spark):
    """Grain: one row per term x course x exam type."""

    per_course = (gold(spark, "fact_exam_result")
                  .groupBy("term_id", "course_id", "exam_type")
                  .agg(
                      F.count("*").alias("students"),
                      F.sum((F.col("result_status") == "absent").cast("int")).alias("absent"),
                      F.sum("is_pass").alias("passed"),
                      F.round(F.avg("score_pct"), 2).alias("avg_score_pct"),
                  ))

    per_course = (per_course
        .withColumn("appeared", F.col("students") - F.col("absent"))
        .withColumn("pass_rate_pct", F.round(F.col("passed") / F.col("appeared") * 100, 2)))

    courses = gold(spark, "dim_course").select("course_id", "course_code", "course_title", "department_name")
    terms = gold(spark, "dim_term").select("term_id", "term_name", "academic_year", "start_date")

    df = (per_course
        .join(courses, on="course_id", how="inner")
        .join(terms, on="term_id", how="inner"))

    return df.select(
        "academic_year", "term_name", "start_date", "department_name", "course_code", "course_title",
        "exam_type", "students", "absent", "appeared", "passed", "pass_rate_pct", "avg_score_pct",
        "term_id", "course_id",
    )

MARTS = {
    "mart_attendance_eligibility": build_mart_attendance_eligibility,
    "mart_fee_aging": build_mart_fee_aging,
    "mart_course_results": build_mart_course_results,
}

# ---------------------------------------------------------------- run

def main():
    spark = get_spark("gold")
    for name, build in DIMENSIONS.items():
        save(build(spark), name)
        print(f"{name:<22} {spark.read.format('delta').load(f'{GOLD}/{name}').count():>10,} rows")

    build_dim_student(spark)
    dim = spark.read.format("delta").load(f"{GOLD}/dim_student")
    print(f"{'dim_student':<22} {dim.count():>10,} rows ({dim.filter('is_current').count():,} current)")

    for name, build in FACTS.items():
        save(build(spark), name)
        print(f"{name:<22} {spark.read.format('delta').load(f'{GOLD}/{name}').count():>10,} rows")

    for name, build in MARTS.items():
            save(build(spark), name)
            print(f"{name:<22} {spark.read.format('delta').load(f'{GOLD}/{name}').count():>10,} rows")

    spark.stop()


if __name__ == "__main__":
    main()