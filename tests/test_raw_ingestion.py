"""
Task 1 - Raw data ingestion tests.

Each test re-runs the actual loader functions from the 01_raw_utils
library notebook (src/utils/) and then re-reads the original source file independently.
"""

import re

import pytest
from pyspark.sql import functions as F

SOURCE_DIR = "/Volumes/workspace/default/peiassignmentdata"


@pytest.fixture(scope="module", autouse=True)
def run_ingestion(dlm):
    """Re-run the real ingestion functions so the tests validate what they produce."""
    dlm.create_schemas()
    dlm.load_raw_cust_data()
    dlm.load_raw_products_data()
    dlm.load_raw_orders_data()


def _read_source_customers(spark):
    """Independent re-read of Customer.xlsx (same sheet and header options as the loader)."""
    return (
        spark.read
        .option("headerRows", 1)
        .option("dataAddress", "Worksheet")
        .excel(f"{SOURCE_DIR}/Customer.xlsx")
    )


def _read_source_products(spark):
    """Independent re-read of Products.csv."""
    return (
        spark.read
        .option("header", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("inferSchema", "false")
        .csv(f"{SOURCE_DIR}/Products.csv")
    )


def _read_source_orders(spark):
    """Independent re-read of Orders.json (multiline records)."""
    return spark.read.option("multiline", "true").json(f"{SOURCE_DIR}/Orders.json")


def _assert_table_equals_source(spark, table, source, dlm):
    """Assert the raw table contains exactly the source rows with normalized column names.

    Uses a scalable fingerprint check instead of an exceptAll diff, which shuffles
    and materializes both datasets twice: compare the column list, the row count,
    and an order-insensitive checksum (sum of per-row xxhash64 over every column,
    NULLs marked).
    """
    normalized = dlm.clean_cols(source)
    actual = spark.table(table)

    assert actual.columns == normalized.columns, (
        f"{table} column list differs from the source file: "
        f"{actual.columns} != {normalized.columns}"
    )

    def _fingerprint(df):
        hashed_cols = [
            F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in df.columns
        ]
        return (
            df.select(F.xxhash64(*hashed_cols).alias("row_hash"))
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                F.coalesce(
                    F.sum(F.col("row_hash").cast("decimal(38,0)")),
                    F.lit(0).cast("decimal(38,0)"),
                ).alias("checksum"),
            )
            .collect()[0]
        )

    table_fp, source_fp = _fingerprint(actual), _fingerprint(normalized)

    assert table_fp["row_count"] == source_fp["row_count"], (
        f"{table} has {table_fp['row_count']} rows but the source file has {source_fp['row_count']}"
    )
    assert table_fp["checksum"] == source_fp["checksum"], (
        f"{table} rows do not match the source file (row-hash checksum mismatch)"
    )


def test_raw_customers_table_matches_source(dlm, spark):
    """raw.customers contains every Customer.xlsx row, unmodified, with normalized columns."""
    _assert_table_equals_source(spark, "raw.customers", _read_source_customers(spark), dlm)


def test_raw_products_table_matches_source(dlm, spark):
    """raw.products contains every Products.csv row, values kept as strings."""
    _assert_table_equals_source(spark, "raw.products", _read_source_products(spark), dlm)


def test_raw_orders_table_matches_source(dlm, spark):
    """raw.orders contains exactly one row per Orders.json record."""
    _assert_table_equals_source(spark, "raw.orders", _read_source_orders(spark), dlm)


def test_raw_column_names_are_normalized(spark):
    """Every raw table's columns are lowercase snake_case (clean_cols applied on load)."""
    for table in ["raw.customers", "raw.products", "raw.orders"]:
        for col in spark.table(table).columns:
            assert re.fullmatch(r"[a-z][a-z0-9_]*", col), f"{table}: column '{col}' is not normalized"


def test_raw_layer_preserves_dirty_source_values(spark):
    """The raw layer is a landing zone: source dirt (digits in names, phone punctuation and
    extensions) must survive untouched for auditability."""
    names_with_junk = spark.table("raw.customers").where("customer_name rlike '[0-9]'").count()
    phones_with_junk = spark.table("raw.customers").where("phone rlike '[^0-9]'").count()
    assert names_with_junk > 0, "raw.customers should still contain uncleaned names"
    assert phones_with_junk > 0, "raw.customers should still contain uncleaned phones"
