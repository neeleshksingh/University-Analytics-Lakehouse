from datetime import datetime

from src.lakehouse import gold, storage

STUDENT = ("student_id int, roll_no string, first_name string, last_name string, gender string, "
           "admission_year int, program_id int, city string, state string, status string, updated_at timestamp")


def test_scd2_versions_only_tracked_changes(spark, tmp_path, monkeypatch):
    # Point all storage at a temporary folder, never at your real lake
    monkeypatch.setattr(storage, "LAKE_ROOT", str(tmp_path))

    # Day 1: first load
    day1 = spark.createDataFrame([
        (1, "R1", "Asha", "Roy", "F", 2024, 10, "Ranchi", "Jharkhand", "active", datetime(2026, 1, 1)),
        (2, "R2", "Ravi", "Sen", "M", 2024, 10, "Patna", "Bihar", "active", datetime(2026, 1, 1)),
    ], STUDENT)
    monkeypatch.setattr(gold, "silver", lambda spark, name: day1)   # feed fake Silver data
    gold.build_dim_student(spark)

    # Day 2: student 1 moved city within the same state (tracked); student 2 changed something untracked
    day2 = spark.createDataFrame([
        (1, "R1", "Asha", "Roy", "F", 2024, 10, "Jamshedpur", "Jharkhand", "active", datetime(2026, 2, 1)),
        (2, "R2", "Ravi", "Sen", "M", 2024, 10, "Patna", "Bihar", "active", datetime(2026, 2, 1)),
    ], STUDENT)
    monkeypatch.setattr(gold, "silver", lambda spark, name: day2)
    gold.build_dim_student(spark)

    dim = spark.read.format("delta").load(f"{tmp_path}/gold/dim_student")
    asha = dim.filter("student_id = 1").orderBy("valid_from").collect()
    ravi = dim.filter("student_id = 2").collect()

    assert [r["city"] for r in asha] == ["Ranchi", "Jamshedpur"]    # history kept
    assert [r["is_current"] for r in asha] == [False, True]         # only the new one is current
    assert asha[0]["valid_to"] == asha[1]["valid_from"]             # no gap, no overlap
    assert len(ravi) == 1 and ravi[0]["is_current"]                 # untracked change: no new version