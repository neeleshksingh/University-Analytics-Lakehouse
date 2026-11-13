"""Where tables live. The only module that knows local folders from Databricks table names.

    LAKEHOUSE_TARGET=local       (default) Delta folders under lake/
    LAKEHOUSE_TARGET=databricks  Unity Catalog tables: <catalog>.uni_<layer>.<name>
"""
import os

from delta.tables import DeltaTable

TARGET = os.environ.get("LAKEHOUSE_TARGET", "local")
CATALOG = os.environ.get("UC_CATALOG", "workspace")
LAKE_ROOT = os.environ.get("LAKE_ROOT", "lake")


def on_databricks():
    return TARGET == "databricks"


def location(layer, name):
    """layer is bronze, silver, gold or ops."""
    if on_databricks():
        return f"{CATALOG}.uni_{layer}.{name}"
    return f"{LAKE_ROOT}/{layer}/{name}"


def landing_root():
    """Where the extract's Parquet files are."""
    if on_databricks():
        return f"/Volumes/{CATALOG}/uni_raw/landing"
    return f"{LAKE_ROOT}/landing"


def read(spark, layer, name):
    if on_databricks():
        return spark.table(location(layer, name))
    return spark.read.format("delta").load(location(layer, name))


def exists(spark, layer, name):
    if on_databricks():
        return spark.catalog.tableExists(location(layer, name))
    return DeltaTable.isDeltaTable(spark, location(layer, name))


def delta_table(spark, layer, name):
    """For MERGE."""
    if on_databricks():
        return DeltaTable.forName(spark, location(layer, name))
    return DeltaTable.forPath(spark, location(layer, name))


def write(df, layer, name, mode="append", overwrite_schema=False):
    writer = df.write.format("delta").mode(mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    if on_databricks():
        writer.saveAsTable(location(layer, name))
    else:
        writer.save(location(layer, name))