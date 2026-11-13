from datetime import datetime

from src.lakehouse.silver import latest_rows


def test_latest_rows_keeps_newest_version(spark):
    rows = [
        (1, "old",   datetime(2026, 1, 1), datetime(2026, 1, 2)),
        (1, "new",   datetime(2026, 1, 5), datetime(2026, 1, 6)),   # newer updated_at wins
        (2, "first", datetime(2026, 1, 3), datetime(2026, 1, 3)),
        (2, "retry", datetime(2026, 1, 3), datetime(2026, 1, 9)),   # same updated_at, ingested later wins
    ]
    df = spark.createDataFrame(rows, "id int, value string, updated_at timestamp, _ingested_at timestamp")

    result = {r["id"]: r["value"] for r in latest_rows(df, "id").collect()}

    assert result == {1: "new", 2: "retry"}