"""Export the Gold marts to CSV files in exports/ (a backup way to open them outside Spark)."""
from pathlib import Path

from src.lakehouse import storage
from src.lakehouse.gold import MARTS
from src.lakehouse.spark import get_spark, stop_spark

EXPORT = Path("exports")


def main():
    spark = get_spark("export")
    EXPORT.mkdir(exist_ok=True)
    for name in MARTS:
        df = storage.read(spark, "gold", name).toPandas()
        path = EXPORT / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"{name:<30} {len(df):>10,} rows -> {path}")
    stop_spark(spark)


if __name__ == "__main__":
    main()