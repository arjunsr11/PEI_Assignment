"""
Tasks 4 & 5 - Aggregation tests.

Validates curated.profit_aggregates against an independent recomputation of
the same grain, checks the 2-decimal rounding required by the task, exercises
create_profit_aggregates on a small known fixture, and executes the Task 5
SQL files to confirm they reproduce the expected totals.
"""

from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

SQL_DIR = Path("/Workspace/PEI Assignment/sql_queries")
GROUP_COLS = ["order_year", "category", "sub_category", "customer_name"]


@pytest.fixture(scope="module", autouse=True)
def ensure_curated_tables(dlm, spark):
    """Aggregation tests read curated.*; build any missing upstream tables first (idempotent)."""
    try:
        spark.table("curated.orders")
        return
    except Exception:
        pass
    dlm.create_schemas()
    dlm.load_raw_cust_data()
    dlm.load_raw_products_data()
    dlm.load_raw_orders_data()
    dlm.load_enriched_cust_data()
    dlm.load_enriched_product_data()
    dlm.load_enriched_orders_data(dlm.clean_raw_orders_data(spark.table("raw.orders")))
    dlm.load_curated_orders_data(
        spark.table("enriched.orders"),
        spark.table("enriched.customers"),
        spark.table("enriched.products"),
    )


def _row_fingerprint(df):
    """Scalable order-insensitive fingerprint: (row_count, checksum).

    The checksum sums per-row xxhash64 hashes over every column (NULLs marked).
    """
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


def test_profit_aggregates_match_independent_recomputation(spark):
    """Task 4: the stored aggregate equals the same grouping recomputed in SQL from curated.orders.

    Verified with the scalable fingerprint above (column list, group count,
    row-hash checksum) so the check stays cheap even on large tables."""
    expected = spark.sql(
        """
        SELECT order_year, category, sub_category, customer_name,
               CAST(ROUND(SUM(profit), 2) AS STRING) AS profit
        FROM curated.orders
        GROUP BY order_year, category, sub_category, customer_name
        """
    )
    actual = (
        spark.table("curated.profit_aggregates")
        .select(*GROUP_COLS, F.col("profit").cast("string").alias("profit"))
    )

    assert actual.columns == expected.columns, (
        f"profit_aggregates column list differs from the recomputation: "
        f"{actual.columns} != {expected.columns}"
    )

    actual_fp, expected_fp = _row_fingerprint(actual), _row_fingerprint(expected)

    assert actual_fp["row_count"] == expected_fp["row_count"], (
        f"profit_aggregates has {actual_fp['row_count']} groups but the recomputation has {expected_fp['row_count']}"
    )
    assert actual_fp["checksum"] == expected_fp["checksum"], (
        "profit_aggregates has groups/values that differ from the recomputation "
        "(row-hash checksum mismatch)"
    )


def test_profit_aggregates_total_reconciles_with_curated_orders(spark):
    """Task 4: no profit is lost or invented between curated.orders and the aggregate."""
    agg_total = spark.table("curated.profit_aggregates").groupBy().sum("profit").first()[0]
    orders_total = spark.table("curated.orders").groupBy().sum("profit").first()[0]
    assert abs(agg_total - orders_total) < Decimal("0.005"), f"aggregate {agg_total} != orders {orders_total}"


def test_profit_is_stored_rounded_to_two_decimals(spark):
    """The task requires profit rounded to 2 decimal places; the stored aggregate is an exact 2-scale decimal."""
    profit_type = dict(spark.table("curated.profit_aggregates").dtypes)["profit"]
    assert profit_type.startswith("decimal(") and profit_type.endswith(",2)"), profit_type


@pytest.fixture()
def restore_profit_aggregates(dlm, spark):
    """curated.profit_aggregates is overwritten by the fixture test; restore real data after."""
    yield
    dlm.create_profit_aggregates(spark.table("curated.orders"))


def test_create_profit_aggregates_computes_group_sums(dlm, spark, restore_profit_aggregates):
    """Task 4: on a known fixture, groups collapse and profits sum with 2-decimal rounding."""
    fixture = spark.createDataFrame(
        [
            (2014, "Furniture", "Chairs", "Adam Hart", Decimal("10.10")),
            (2014, "Furniture", "Chairs", "Adam Hart", Decimal("20.20")),   # same group: 30.30
            (2014, "Furniture", "Chairs", "Mary O'Rourke", Decimal("1.05")),
            (2015, "Technology", "Phones", "Becky Pak", Decimal("-5.55")),
        ],
        "order_year int, category string, sub_category string, customer_name string, profit decimal(10,2)",
    )
    dlm.create_profit_aggregates(fixture)

    rows = sorted(
        (r["order_year"], r["category"], r["sub_category"], r["customer_name"], str(r["profit"]))
        for r in spark.table("curated.profit_aggregates").collect()
    )
    assert rows == [
        (2014, "Furniture", "Chairs", "Adam Hart", "30.30"),
        (2014, "Furniture", "Chairs", "Mary O'Rourke", "1.05"),
        (2015, "Technology", "Phones", "Becky Pak", "-5.55"),
    ]


# ---------------- Task 5: SQL query files ----------------

@pytest.mark.parametrize("sql_file, source_filter", [
    ("01_profit_by_year.sql", None),
    ("02_profit_by_year_and_category.sql", None),
    ("03_profit_by_customer.sql", "customer_name IS NOT NULL"),
    ("04_profit_by_customer_and_year.sql", "customer_name IS NOT NULL"),
])
def test_sql_query_file_reproduces_expected_total(spark, sql_file, source_filter):
    """Each Task 5 SQL file aggregates the full profit for its scope."""
    result = spark.sql((SQL_DIR / sql_file).read_text())
    assert result.count() > 0, f"{sql_file} returned no rows"

    sql_total = result.groupBy().sum("total_profit").first()[0]

    source = spark.table("curated.orders")
    if source_filter:
        source = source.where(source_filter)
    expected = source.groupBy().sum("profit").first()[0]
    assert abs(sql_total - expected) < Decimal("0.01"), f"{sql_file}: SQL total {sql_total} != expected {expected}"


def test_sql_profit_by_year_covers_every_order_year(spark):
    """Task 5a: one output row per calendar year present in curated.orders."""
    sql_years = {r[0] for r in spark.sql((SQL_DIR / "01_profit_by_year.sql").read_text()).collect()}
    table_years = {r[0] for r in spark.table("curated.orders").select("order_year").distinct().collect()}
    assert sql_years == table_years
