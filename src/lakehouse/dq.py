from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Each check describes a VALID row, as a Spark SQL condition.
RULES = [
    dict(table="student", name="email_present", severity="warn",
         check="email IS NOT NULL"),
    dict(table="student", name="phone_valid_mobile", severity="warn",
         check="phone RLIKE '^[6-9][0-9]{9}$'"),
    dict(table="faculty", name="email_present", severity="warn",
         check="email IS NOT NULL"),
    dict(table="payment", name="success_amount_positive", severity="reject",
         check="status <> 'success' OR amount > 0"),
    dict(table="exam_result", name="marks_within_max", severity="reject",
         check="marks_obtained IS NULL OR marks_obtained <= max_marks"),
    dict(table="attendance", name="student_registered_in_class", severity="reject",
        check="_dq_is_registered"),
]


def apply_rules(df, table_name, key, run_id):
    """Return (rows allowed into Silver, quarantined rows or None, result rows for the scorecard)."""
    rules = [r for r in RULES if r["table"] == table_name]
    if not rules:
        return df, None, []

    df = df.cache()                    
    total = df.count()
    quarantine_parts, results = [], []
    all_rejects_pass = F.lit(True)

    for rule in rules:
        passed = F.coalesce(F.expr(rule["check"]), F.lit(False))

        failed = df.filter(~passed)
        n_failed = failed.count()
        results.append((run_id, table_name, rule["name"], rule["severity"], total, n_failed))

        if n_failed:
            quarantine_parts.append(failed.select(
                F.lit(table_name).alias("table_name"),
                F.col(key).cast("string").alias("record_key"),
                F.lit(rule["name"]).alias("rule"),
                F.lit(rule["severity"]).alias("severity"),
                F.to_json(F.struct(*df.columns)).alias("record"),
                F.lit(run_id).alias("run_id"),
            ))

        if rule["severity"] == "reject":
            all_rejects_pass = all_rejects_pass & passed

    allowed = df.filter(all_rejects_pass)
    quarantine = reduce(DataFrame.unionByName, quarantine_parts) if quarantine_parts else None
    return allowed, quarantine, results