"""
Tasks 2 & 3 - Data enrichment tests.

Unit tests feed known dirty fixtures through the ACTUAL functions from the
02_enriched_utils library notebook (src/utils/) and assert the cleaned output; integration tests
re-run the enriched loads and validate the tables they produce.
"""

from datetime import date

import pytest
from pyspark.sql import types as T


def apply_fn(spark, fn, col_name, values):
    """Apply a Column-expression function to raw values and return the outputs."""
    df = spark.createDataFrame([(v,) for v in values], f"{col_name} string")
    return [r[0] for r in df.select(fn(col_name).alias("out")).collect()]


# ---------------- Task 2: customer name cleaning ----------------

@pytest.mark.parametrize("raw_name, expected", [
    ("Mary O'Rourke", "Mary O'Rourke"),          # genuine apostrophe is preserved
    ("Ad.       ..am Hart", "Adam Hart"),        # junk runs and inner spaces removed
    ("Tam&^*ara Willing___)ham", "Tamara Willingham"),
    ("Kat rina Bavinge", "Katrina Bavinge"),     # gap between lowercase letters is corruption
    ("   Kristi;'[]na Nunn", "Kristina Nunn"),   # apostrophe before a lowercase letter is corruption
    ("Gary567 Hansen", "Gary Hansen"),          # digits inside a name are removed
    ("Peter Bühler", "Peter Bühler"),            # non-ASCII letters survive
    (None, None),                                # a missing name stays null
])
def test_clean_name(dlm, spark, raw_name, expected):
    """clean_name produces the expected value for known dirty inputs."""
    out = apply_fn(spark, dlm.clean_name, "customer_name", [raw_name])
    assert out == [expected], f"clean_name({raw_name!r}) -> {out!r}, expected {expected!r}"


# ---------------- Task 2: phone cleaning ----------------

@pytest.mark.parametrize("raw_phone, expected", [
    ("421.580.0902x9815", "+14215800902"),       # extension + punctuation stripped
    ("001-542-415-0246x314", "+15424150246"),    # leading 001 country code removed
    ("7185624866", "+17185624866"),              # already-clean 10-digit number
    ("-6181", None),                              # too short -> null
    ("#ERROR!", None),                           # no digits at all -> null
    (None, None),                                 # missing phone stays null
])
def test_clean_phone(dlm, spark, raw_phone, expected):
    """clean_phone produces the expected value for known dirty inputs."""
    out = apply_fn(spark, dlm.clean_phone, "phone", [raw_phone])
    assert out == [expected], f"clean_phone({raw_phone!r}) -> {out!r}, expected {expected!r}"


# ---------------- Task 3: order cleaning ----------------

ORDERS_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("order_date", T.StringType()),   # raw layer keeps dates as strings
    T.StructField("ship_date", T.StringType()),
    T.StructField("ship_mode", T.StringType()),
    T.StructField("discount", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("price", T.StringType()),
    T.StructField("profit", T.StringType()),
    T.StructField("row_id", T.StringType()),
])


def _orders_fixture(spark):
    """Small raw-orders fixture: a duplicate order line, day-first dates, an unknown customer/product."""
    rows = [
        # latest version of (CA-2017-001, FUR-CH-1): row_id 2, later date -> the one to keep
        ("CA-2017-001", "C-01", "FUR-CH-1", "25/12/2017", "28/12/2017", "Standard Class", "0.20", "3", "150.00", "40.00", "2"),
        # older duplicate of the same order line -> must be dropped
        ("CA-2017-001", "C-01", "FUR-CH-1", "21/12/2017", "24/12/2017", "Standard Class", "0.10", "2", "100.00", "10.00", "1"),
        # day-first single-digit dates: 5/11/2017 -> 2017-11-05
        ("CA-2017-002", "C-02", "TEC-PH-2", "5/11/2017", "7/11/2017", "First Class", "0.00", "1", "200.00", "-5.50", "3"),
        # different product within the same order -> not a duplicate
        ("CA-2017-002", "C-02", "OFF-BI-3", "5/11/2017", "7/11/2017", "First Class", "0.00", "5", "20.00", "6.75", "4"),
        # unknown customer and product -> must survive into curated via left joins
        ("CA-2017-003", "C-99", "UNKNOWN-9", "1/1/2017", "2/1/2017", "Standard Class", "0.00", "1", "10.00", "2.00", "5"),
    ]
    return spark.createDataFrame(rows, ORDERS_SCHEMA)


def test_clean_orders_parses_day_first_dates(dlm, spark):
    """Order and ship dates are parsed day-first ('5/11/2017' is 5 Nov 2017, not 11 May)."""
    cleaned = dlm.clean_raw_orders_data(_orders_fixture(spark))
    row = cleaned.where("order_id = 'CA-2017-002' AND product_id = 'TEC-PH-2'").first()
    assert row["order_date"] == date(2017, 11, 5)
    assert row["ship_date"] == date(2017, 11, 7)


def test_clean_orders_keeps_latest_duplicate_order_line(dlm, spark):
    """Duplicate (order_id, product_id) lines collapse to their most recent version."""
    cleaned = dlm.clean_raw_orders_data(_orders_fixture(spark))
    assert cleaned.count() == 4, "5 rows with one duplicate pair must leave 4"
    kept = cleaned.where("order_id = 'CA-2017-001'").first()
    assert kept["row_id"] == 2, "the later version (row_id 2) must win"
    assert str(kept["profit"]) == "40.00", "the surviving row must be the latest one"


def test_clean_orders_casts_numeric_columns(dlm, spark):
    """price/profit/discount become decimal(10,2) (2-decimal precision) and quantity becomes int."""
    dtypes = dict(dlm.clean_raw_orders_data(_orders_fixture(spark)).dtypes)
    assert dtypes["price"] == "decimal(10,2)"
    assert dtypes["profit"] == "decimal(10,2)"
    assert dtypes["discount"] == "decimal(10,2)"
    assert dtypes["quantity"] == "int"


# ---------------- Tasks 2 & 3: enriched tables (integration) ----------------

@pytest.fixture(scope="module", autouse=True)
def ensure_pipeline_tables(dlm, spark):
    """The enrichment functions read raw.*; build any missing upstream tables first (idempotent)."""
    def _exists(name):
        try:
            spark.table(name)
            return True
        except Exception:
            return False

    if not _exists("raw.orders"):
        dlm.create_schemas()
        dlm.load_raw_cust_data()
        dlm.load_raw_products_data()
        dlm.load_raw_orders_data()
    if not _exists("enriched.orders"):
        dlm.load_enriched_cust_data()
        dlm.load_enriched_product_data()
        dlm.load_enriched_orders_data(dlm.clean_raw_orders_data(spark.table("raw.orders")))
    if not _exists("curated.orders"):
        dlm.load_curated_orders_data(
            spark.table("enriched.orders"),
            spark.table("enriched.customers"),
            spark.table("enriched.products"),
        )


def test_enriched_customers_are_cleaned_and_deduplicated(dlm, spark):
    """Task 2: enriched.customers keeps every source customer exactly once, with cleaned names and phones."""
    dlm.load_enriched_cust_data()
    customers = spark.table("enriched.customers")

    raw_ids = spark.table("raw.customers").select("customer_id").distinct().count()
    assert customers.count() == raw_ids, "every source customer must be kept exactly once"

    dirty_names = customers.where("customer_name rlike '[0-9]' OR customer_name rlike '[&^*#%]' ").count()
    assert dirty_names == 0, "cleaned names must not contain digits or junk characters"

    # bracket class avoids backslash escapes, which Spark SQL string literals would consume
    bad_phones = customers.where("phone IS NOT NULL AND phone NOT rlike '^[+]1[0-9]{10}$'").count()
    assert bad_phones == 0, "every kept phone must be a +1-prefixed 10-digit number"


def test_enriched_products_are_typed_and_deduplicated(dlm, spark):
    """Task 2: enriched.products has a 2-decimal price, no null keys, and one row per product."""
    dlm.load_enriched_product_data()
    products = spark.table("enriched.products")

    assert dict(products.dtypes)["price_per_product"] == "decimal(10,2)"
    assert products.where("product_id IS NULL").count() == 0

    raw_ids = spark.table("raw.products").where("product_id IS NOT NULL").select("product_id").distinct().count()
    assert products.count() == raw_ids, "every source product must be kept exactly once"


# ---------------- Task 3: curated master table ----------------

@pytest.fixture()
def restore_curated_orders(dlm, spark):
    """curated.orders is overwritten by the master-table test; restore the real data afterwards."""
    yield
    dlm.load_curated_orders_data(
        spark.table("enriched.orders"),
        spark.table("enriched.customers"),
        spark.table("enriched.products"),
    )


def test_curated_master_table_joins_customers_and_products(dlm, spark, restore_curated_orders):
    """Task 3: curated.orders carries customer name/country and category/sub-category, keeps
    orders with unknown customers/products (left joins), and derives order_year."""
    customers = spark.createDataFrame(
        [("C-01", "Mary O'Rourke", "United States"), ("C-02", "Becky Pak", "Canada")],
        "customer_id string, customer_name string, country string")
    products = spark.createDataFrame(
        [("FUR-CH-1", "Furniture", "Chairs"), ("TEC-PH-2", "Technology", "Phones"),
         ("OFF-BI-3", "Office Supplies", "Binders")],
        "product_id string, category string, sub_category string")

    # like the real pipeline, the curated layer is built from the CLEANED (typed) orders:
    # the duplicate order line collapses first, then the left joins add customer/product attributes
    orders_clean = dlm.clean_raw_orders_data(_orders_fixture(spark))
    dlm.load_curated_orders_data(orders_clean, customers, products)
    curated = spark.table("curated.orders")

    assert curated.count() == 4, "left joins must keep every cleaned order line"

    known = curated.where("order_id = 'CA-2017-001'").first()
    assert known["customer_name"] == "Mary O'Rourke"
    assert known["country"] == "United States"
    assert known["category"] == "Furniture"
    assert known["sub_category"] == "Chairs"
    assert known["order_year"] == 2017

    unknown = curated.where("order_id = 'CA-2017-003'").first()
    assert unknown["customer_name"] is None and unknown["category"] is None, \
        "orders with unknown customer/product must survive with null attributes"
