"""
Create the source database and seed the ETL control table.

    python -m src.source_db.create_schema           # create what is missing
    python -m src.source_db.create_schema --reset   # drop everything, start fresh
"""
import argparse
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, select, text

from src.source_db.schema import (
    FULL_LOAD_TABLES, SOURCE_TABLES, ingestion_control, metadata,
)


def ensure_etl_schema(conn):
    """The 'etl' schema must exist before etl.ingestion_control can be created."""
    if conn.dialect.name == "postgresql":
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl"))
    elif conn.dialect.name == "mssql":
        conn.execute(text("IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl')"))


def seed_control_table(conn):
    """Add one control row per source table. Existing rows are never touched."""
    existing = set(conn.execute(select(ingestion_control.c.source_table)).scalars())

    new_rows = []
    for table in SOURCE_TABLES:
        if table.name in existing:
            continue

        new_rows.append({
            "source_table" : table.name,
            "load_type" : "full" if table.name in FULL_LOAD_TABLES else "incremental",
            "primary_key" : list(table.primary_key.columns)[0].name
        })

    if new_rows:
        conn.execute(ingestion_control.insert(), new_rows)
    return len(new_rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="drop all tables first")
    args = parser.parse_args()

    load_dotenv()
    engine = create_engine(os.environ["SOURCE_DB_URL"])

    with engine.begin() as conn:
        ensure_etl_schema(conn)
        if args.reset:
            metadata.drop_all(conn)
        metadata.create_all(conn)
        added = seed_control_table(conn)

    inspector = inspect(engine)
    print(f"Source tables:      {len(inspector.get_table_names())}")
    print(f"Control rows added: {added}")


if __name__ == "__main__":
    main()