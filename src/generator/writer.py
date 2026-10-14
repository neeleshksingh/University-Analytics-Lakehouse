"""
Fast bulk loading into the source database.

Inserting millions of rows one INSERT at a time takes hours. Instead:
  - PostgreSQL: COPY ... FROM STDIN streams rows in one command (millions per minute)
  - Other databases (Azure SQL): batched executemany, 5,000 rows per round trip

The generator assigns primary keys itself (explicit IDs), so foreign keys can be
built in memory without reading IDs back from the database. After a bulk load,
`sync_identity` moves each table's auto-increment counter past the highest ID.
"""

from itertools import islice

from sqlalchemy import func, select, text

BATCH = 5000


class BulkWriter:
    def __init__(self, engine):
        self.engine = engine
        self.dialect = engine.dialect.name
        self.counts = {}

    def write(self, table, columns, rows):
        """Write an iterable of tuples (in `columns` order) into `table`. Returns rows written."""
        if self.dialect == "postgresql":
            written = self._copy_postgres(table, columns, rows)
        else:
            written = self._executemany(table, columns, rows)
        self.counts[table.name] = self.counts.get(table.name, 0) + written
        return written

    def _copy_postgres(self, table, columns, rows):
        raw = self.engine.raw_connection()
        written = 0
        try:
            cols = ", ".join(columns)
            with raw.driver_connection.cursor() as cur:
                with cur.copy(f"COPY {table.name} ({cols}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)
                        written += 1
            raw.commit()
        finally:
            raw.close()
        return written

    def _executemany(self, table, columns, rows):
        written = 0
        iterator = iter(rows)
        with self.engine.begin() as conn:
            while True:
                chunk = list(islice(iterator, BATCH))
                if not chunk:
                    break
                conn.execute(table.insert(), [dict(zip(columns, r)) for r in chunk])
                written += len(chunk)
        return written


def sync_identity(engine, tables):
    """After explicit-ID inserts, move each identity counter past the current maximum ID."""
    with engine.begin() as conn:
        for table in tables:
            pk = list(table.primary_key.columns)[0]
            if engine.dialect.name == "postgresql":
                conn.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('{table.name}', '{pk.name}'), "
                    f"COALESCE((SELECT MAX({pk.name}) FROM {table.name}), 1))"
                ))
            elif engine.dialect.name == "mssql":
                conn.execute(text(f"DBCC CHECKIDENT ('{table.name}', RESEED)"))


def next_id(conn, table):
    """Next free primary key value, used by delta mode for explicit IDs."""
    pk = list(table.primary_key.columns)[0]
    return (conn.execute(select(func.max(pk))).scalar() or 0) + 1
