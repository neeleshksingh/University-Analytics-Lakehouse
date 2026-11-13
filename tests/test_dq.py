from src.lakehouse.dq import apply_rules


def test_reject_rule_quarantines_bad_payment(spark):
    rows = [
        (1, "success", 100.0),   # good
        (2, "success", -50.0),   # bad: a successful payment can't be negative
        (3, "failed", 0.0),      # fine: the rule only checks successful payments
    ]
    df = spark.createDataFrame(rows, "payment_id int, status string, amount double")

    allowed, quarantine, results = apply_rules(df, "payment", "payment_id", "test")

    # allowed is a DataFrame: only the valid payments go on to Silver
    assert {r["payment_id"] for r in allowed.collect()} == {1, 3}

    # quarantine is a DataFrame: the bad row, with its key stored as a string
    assert quarantine.count() == 1
    assert quarantine.first()["record_key"] == "2"

    # results is a Python list of tuples: one entry per rule that ran
    assert len(results) == 1
    assert results[0][-1] == 1      # last value in the tuple = rows_failed