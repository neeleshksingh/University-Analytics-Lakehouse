from sqlalchemy import (
    MetaData, Table, Column,
    Integer, String, Boolean, DateTime,
    Identity, func, false,
)

metadata = MetaData()


def audit_columns():
    """The three columns every source table gets."""
    return [
        Column("created_at", DateTime, nullable=False, server_default=func.now()),
        Column("updated_at", DateTime, nullable=False, server_default=func.now()),
        Column("is_deleted", Boolean, nullable=False, server_default=false()),
    ]


department = Table(
    "department", metadata,
    Column("department_id", Integer, Identity(), primary_key=True),
    Column("dept_code", String(10), nullable=False, unique=True),
    Column("name", String(100), nullable=False),
    *audit_columns(),
)


if __name__ == "__main__":
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.dialects import postgresql, mssql

    for table in metadata.sorted_tables:
        print(CreateTable(table).compile(dialect=postgresql.dialect()))