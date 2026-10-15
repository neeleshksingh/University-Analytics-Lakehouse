"""
Incremental extract: copy changed rows from the source database into Parquet files.

    python -m src.ingestion.extract --dry-run   # only show what would be copied
"""
import argparse
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, func, select

from src.source_db import schema as s

from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import update

LAKE = Path("lake/landing")
CHUNK_ROWS = 200_000


def read_control_rows(conn):
    """Step 1: read the 'notebook': one row per active source table."""
    ctl = s.ingestion_control
    return conn.execute(
        select(ctl).where(ctl.c.is_active.is_(True)).order_by(ctl.c.source_table)
    ).all()


def build_query(table, control, upper_bound):
    """Step 2: decide which rows to copy for one table."""
    query = select(table)
    if control.load_type == "incremental":
        watermark_col = table.c[control.watermark_column]
        query = query.where(
            watermark_col > control.last_watermark,
            watermark_col <= upper_bound,
        )
        pass
    return query

def extract_table(conn, table, query, batch_id, run_date):
    """Step 3: copy the query's rows into Parquet files. Returns the number of rows written."""
    folder = LAKE / table.name / f"ingest_date={run_date}" / f"batch_id={batch_id}"
    rows_written = 0
    streaming = conn.execution_options(stream_results=True)

    for part, chunk in enumerate(pd.read_sql(query, streaming, chunksize=CHUNK_ROWS)):
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"part-{part:04d}.parquet"
        chunk.to_parquet(path, index=False)
        rows_written += len(chunk)
        pass

    return rows_written


def record_run(engine, table_name, status, rows, new_watermark=None):
    """Step 4: update the 'notebook'. The watermark only moves when new_watermark is given."""
    ctl = s.ingestion_control
    values = dict(last_run_status=status, last_run_at=datetime.now(), rows_copied=rows)
    if new_watermark is not None:
        values["last_watermark"] = new_watermark
    with engine.begin() as conn:
        conn.execute(update(ctl).where(ctl.c.source_table == table_name).values(**values))
        pass


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    engine = create_engine(os.environ["SOURCE_DB_URL"])

    batch_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    run_date = datetime.now().date()

    with engine.connect() as conn:
        controls = read_control_rows(conn)

    for control in controls:
        table = s.metadata.tables[control.source_table]
        with engine.connect() as conn:
            upper_bound = conn.execute(
                select(func.max(table.c[control.watermark_column]))
            ).scalar()
            if upper_bound is None:                       # empty table: nothing to do
                print(f"{table.name:<22} empty, skipped")
                continue
            query = build_query(table, control, upper_bound)

            if args.dry_run:
                n = conn.execute(select(func.count()).select_from(query.subquery())).scalar()
                print(f"{table.name:<22} would copy {n:,} rows")
                continue

            try:
                n = extract_table(conn, table, query, batch_id, run_date)
            except Exception as error:
                record_run(engine, table.name, "failed", 0)     # Rule 2: watermark NOT moved
                print(f"{table.name:<22} FAILED: {error}")
                continue

        record_run(engine, table.name, "success", n, new_watermark=upper_bound)  # Rule 2: only after success
        print(f"{table.name:<22} copied {n:,} rows")


if __name__ == "__main__":
    main()