"""
Data Load Methods V2 — script version of the "Data Load Methods" notebook.

Intended to run in a Databricks environment (notebook via %run, or a
Lakeflow Job task) where the `spark` session is already available.

Usage:
    from data_load_methods_v2 import *
    create_schemas()
    load_raw_cust_data(); load_raw_product_data(); load_raw_orders_data()
    load_enriched_cust_data(); load_enriched_product_data()
    orders_clean = clean_raw_orders_data(spark.table("raw.orders"))
    load_enriched_orders_data(orders_clean)
    load_curated_orders_data(
        spark.table("enriched.orders"),
        spark.table("enriched.customers"),
        spark.table("enriched.products"),
    )
"""

import pyspark.sql.functions as F

# An imported module does not inherit the notebook-injected `spark` global,
# so bind the active Spark session (or create one for standalone runs).
if "spark" not in globals():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------


def create_schemas():
    """Create the raw, enriched and curated schemas if they do not already exist."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS raw")        # landing zone: source data as loaded
    spark.sql("CREATE SCHEMA IF NOT EXISTS enriched")   # cleaned and typed data
    spark.sql("CREATE SCHEMA IF NOT EXISTS curated")    # business-ready, joined data


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def clean_cols(df):
    """
    Normalize all column names of a DataFrame: strip surrounding whitespace,
    lowercase, and replace spaces and hyphens with underscores.

    Returns a new DataFrame with the renamed columns.
    """
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(c, new_c)
    return df


def clean_name(col_name="customer_name"):
    """
    Returns a Column expression that cleans a name column by:
      1. Removing junk runs — special characters, underscores, and digits —
         together with the spaces around them
      2. Removing apostrophes followed by a lowercase letter (corruption like
         "Kristi'na"), while keeping genuine ones like "O'Rourke"
      3. Collapsing leftover multiple spaces into a single space and trimming
      4. Joining words across a space between two lowercase letters
         ("Kat rina" -> "Katrina") and stripping leftover punctuation
    """
    # 1. drop junk runs (special chars, underscores, digits) and the spaces around them
    cleaned = F.regexp_replace(F.col(col_name), r"\s*[^\p{L}'\s]+(?:\s+[^\p{L}'\s]+)*", "")
    # 2. remove apostrophe when the next letter is lowercase
    cleaned = F.regexp_replace(cleaned, r"(\p{L})\s*'(\p{Ll})", "$1$2")
    # 3. collapse runs of spaces into a single space and trim
    cleaned = F.trim(F.regexp_replace(cleaned, r"\s+", " "))
    # remove space between two lowercase letters
    cleaned = F.regexp_replace(cleaned, r"(\p{Ll})\s+(\p{Ll})", "$1$2")
    # strip leftover punctuation but KEEP apostrophes inside names (O'Rourke)
    cleaned = F.regexp_replace(cleaned, r"[^\p{L}\s']", "")
    # drop double quotes from the names
    return F.regexp_replace(cleaned, r"\"", "")


def clean_phone(col_name="phone"):
    """
    Returns a Column expression that cleans a phone number column by:
      1. Removing the "x" extension and anything after it
      2. Removing all special characters (only digits remain)
      3. Removing the leading "001" country code if present
      4. Keeping the number only if it is exactly 10 digits, otherwise NULL
    """
    # 1. cut off any "x123"-style extension and everything after it
    no_ext = F.regexp_replace(F.col(col_name), r"[xX]\d+.*$", "")
    # 2. strip everything that is not a digit
    digits = F.regexp_replace(no_ext, r"[^0-9]", "")
    # 3. drop a leading "001" country code
    no_cc = F.regexp_replace(digits, r"^001", "")
    # 4. keep only exact 10-digit numbers (prefixed "+1"); everything else becomes NULL
    return F.when(F.length(no_cc) == 10, F.concat(F.lit("+1"), no_cc))


def clean_str_columns(df):
    """
    Returns a DataFrame with every column cast to string and trimmed of
    surrounding whitespace.
    """
    for c, dtype in df.dtypes:
        df = df.withColumn(c, F.trim(F.col(c).cast("string")))
    return df


# ---------------------------------------------------------------------------
# Raw layer loads
# ---------------------------------------------------------------------------


def load_raw_cust_data():
    """
    Load the raw customer data from Customer.xlsx into raw.customers.

    Reads the "Worksheet" sheet using its first row as the header, normalizes
    the column names, and overwrites the raw.customers table (schema included).
    This excel read method requires DBR 17.1+
    """
    raw_cust_df = (
        spark.read
          .option("headerRows", 1)             # first sheet row holds the column names
          .option("dataAddress", "Worksheet")   # sheet to read within the workbook
        .excel("/Volumes/workspace/default/peiassignmentdata/Customer.xlsx")
    )
    raw_cust_df = clean_cols(raw_cust_df)   # normalize column names before persisting
    raw_cust_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.customers")


def load_raw_product_data():
    """
    Load the raw product data from Products.csv into raw.products.

    Reads the CSV with a header row, honoring embedded quotes and escapes, and
    keeps every column as a string (no schema inference). 
    Overwrites raw.products (schema included).
    """
    raw_product_df = (
        spark.read.option("header", "true")
        .option("escape", "\"")        # handle escaped quotes inside values
        .option("quote", "\"")         # double-quote is the quote character
        .option("inferSchema", "false")   # keep all columns as string in the raw layer
        .csv("/Volumes/workspace/default/peiassignmentdata/Products.csv")
    )
    raw_product_df = clean_cols(raw_product_df)   # normalize column names before persisting
    raw_product_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.products")


def load_raw_orders_data():
    """
    Load the raw orders data from Orders.json into raw.orders.

    Reads the JSON in multiline mode because each order record spans multiple
    lines. Column names are normalized and the raw.orders table is overwritten
    (schema included).
    """
    orders_raw = (
        spark.read
        .option("multiline", "true")   # JSON records span multiple lines
        .json("/Volumes/workspace/default/peiassignmentdata/Orders.json")
    )
    orders_raw = clean_cols(orders_raw)   # normalize column names before persisting
    orders_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.orders")


# ---------------------------------------------------------------------------
# Enriched layer loads
# ---------------------------------------------------------------------------


def load_enriched_cust_data():
    """
    Build enriched.customers from raw.customers.

    Cleans customer names and phone numbers, trims all columns, and keeps a
    single row per customer_id before overwriting enriched.customers
    (schema included).
    """
    enriched_cust_df = (
        spark.table("raw.customers")
            .withColumn("customer_name", clean_name("customer_name")) \
            .withColumn("phone", clean_phone("phone"))
    )
    # trim whitespace on all columns
    enriched_cust_df = clean_str_columns(enriched_cust_df)

    # keep one row per customer
    enriched_cust_df = enriched_cust_df.dropDuplicates(["customer_id"])
    enriched_cust_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("enriched.customers")


def load_enriched_product_data():
    """
    Build enriched.products from raw.products.

    Trims string columns, casts price_per_product to decimal(10,2), drops rows
    without a product_id, and keeps a single row per product_id before
    overwriting enriched.products (schema included).
    """
    raw_prod = spark.table("raw.products")

    # trim whitespace on all string columns
    for c, dtype in raw_prod.dtypes:
        if dtype == "string":
            raw_prod = raw_prod.withColumn(c, F.trim(F.col(c)))

    # fixed precision decimal so later aggregations are exact
    enriched_prod = raw_prod.withColumn("price_per_product", F.expr("cast(price_per_product as decimal(10,2))"))
    # discard rows without a product key, then keep one row per product
    enriched_prod = enriched_prod.filter(F.col("product_id").isNotNull()).dropDuplicates(["product_id"])

    enriched_prod.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("enriched.products")


def clean_raw_orders_data(orders_raw):
    """
    Clean and deduplicate the raw orders data.

    Parses order/ship dates (day-first formats), casts the numeric columns,
    trims string columns, and keeps only the latest row per (order_id,
    product_id) pair — ranked by order_date then row_id — so duplicate order
    lines collapse to their most recent version.

    Returns the cleaned, deduplicated DataFrame (not persisted).
    """
    from pyspark.sql.window import Window

    enriched_orders_df = (
        orders_raw
        # day-first dates: try single-digit day first, then zero-padded
        .withColumn("order_date", F.coalesce(
            F.try_to_date("order_date", "d/M/yyyy"),
            F.try_to_date("order_date", "dd/MM/yyyy")))
        .withColumn("ship_date", F.coalesce(
            F.try_to_date("ship_date", "d/M/yyyy"),
            F.try_to_date("ship_date", "dd/MM/yyyy")))
        # cast numerics to proper types for downstream calculations
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        .withColumn("profit", F.col("profit").cast("decimal(10,2)"))
        .withColumn("discount", F.col("discount").cast("decimal(10,2)"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("row_id", F.col("row_id").cast("int"))
    )
    # trim whitespace on all string columns
    for c, dt in enriched_orders_df.dtypes:
        if dt == "string":
            enriched_orders_df = enriched_orders_df.withColumn(c, F.trim(F.col(c)))

    # rank rows within each (order_id, product_id) pair, most recent first
    window = Window.partitionBy("order_id", "product_id").orderBy(F.col("order_date").desc(), F.col("row_id").desc())
    return (
        enriched_orders_df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)   # keep only the latest version of each order line
        .drop("rank")
    )


def load_enriched_orders_data(enriched_orders_df):
    """
    Persist the cleaned orders DataFrame as enriched.orders.

    Selects the final order columns and overwrites the enriched.orders
    table (schema included).
    Inputs: enriched_orders_df
    """
    (
        enriched_orders_df
        .select("order_id", "customer_id", "product_id", "order_date", "ship_date",
                "ship_mode", "discount", "quantity", "price", "profit", "row_id")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("enriched.orders")
    )


# ---------------------------------------------------------------------------
# Curated layer
# ---------------------------------------------------------------------------


def load_curated_orders_data(ord_df, cust_df, prod_df):
    """
    Build curated.orders by joining enriched orders with customer and
    product data.
    Inputs: enriched orders, customers, and products DataFrames
    Overwrites curated.orders
    (schema included).
    """
    # only the attributes needed for the curated layer
    c_sub = cust_df.select("customer_id", "customer_name", "country")
    p_sub = prod_df.select("product_id", "category", "sub_category")
    result = (
        ord_df
        .join(c_sub, "customer_id", "left")   # left join: keep orders even if the customer is missing
        .join(p_sub, "product_id", "left")      # left join: keep orders even if the product is missing
        .select("order_id", "customer_name", "country", "category", "sub_category",
                F.year("order_date").alias("order_year"),   # calendar year for trend analysis
                "profit")
    )
    result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("curated.orders")

def create_profit_aggregates(orders_curated_df):
    """
    Build profit aggregates from the curated orders data.

    Groups curated orders by order year, category, sub_category, and
    customer name, summing profit and rounding to 2 decimal places.

    Ovewrite curated.profit_aggregates
    """
    (
        orders_curated_df
        .groupBy("order_year", "category", "sub_category", "customer_name")
        .agg(F.round(F.sum("profit"), 2).alias("profit"))
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("curated.profit_aggregates")
    )

