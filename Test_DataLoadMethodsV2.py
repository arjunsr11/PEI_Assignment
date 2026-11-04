import sys, importlib
import pytest

sys.path.insert(0, "/Workspace/PEI Assignment")
sys.path_importer_cache.pop("/Workspace/PEI Assignment", None)  # drop any stale directory listing
importlib.invalidate_caches()  # required: the first import can otherwise cache an empty dir

from DataLoadMethodsV2 import clean_name, clean_phone, spark  # the module binds its own Spark session

def apply_fn(fn, col_name, values):
    """Apply a Column-expression function to a list of raw values and return the outputs."""
    df = spark.createDataFrame([(v,) for v in values], f"{col_name} string")  # underscores are junk
    return [r[0] for r in df.select(fn(col_name).alias("out")).collect()]

##---------------- clean_name tests ----------------

@pytest.mark.parametrize("raw, expected", [
    ("Mary O'Rourke", "Mary O'Rourke"),               # genuine apostrophe: uppercase letter follows
    ("Russell D'Ascenzo", "Russell D'Ascenzo"),       # genuine apostrophe
    ("  Mary   O'Rourke 42 ", "Mary O'Rourke"),
    # surrounding junk/spaces are removed
    ("Matthew Willing___ham", "Matthew Willingham"),  # underscores are junk
    ("Ad.       ..am Hart", "Adam Hart"),             # junk run + inner spaces
    ("Tam&^*ara Willing___)ham", "Tamara Willingham"),
    ("Victo 3456ria Brennan", "Victoria Brennan"),    # digits inside words   # digits inside words
    ("Ben Pet 987 -=erman", "Ben Peterman"),
    ("Kat rina Bavinge", "Katrina Bavinge"),         # lowercase-lowercase gap is corruption
    ("   Kristi;'[]na Nunn", "Kristina Nunn"),        # apostrophe between lowercase letters is removed
    ("Gary567 Hansen", "Gary Hansen"),                # word-separating space survives
    ("B ecky Martin", "B ecky Martin"),              # space before an uppercase letter is kept
    ('[]-=;"Becky Pak', "Becky Pak"),                # leading junk punctuation is removed
    ("Peter Bühler", "Peter Bühler"),                 # non-ASCII letters are letters
    (None, None),                                      # a missing name stays null
])
def test_clean_name(raw, expected):
    """Test that clean_name() returns the expected output."""
    out = apply_fn(clean_name, "customer_name", [raw])
    assert out == [expected], f"{out}"

##---------------- clean_phone tests ----------------

@pytest.mark.parametrize("raw, expected", [
    ("421.580.0902x9815", "+14215800902"),      # extension + punctuation stripped, +1 prefixed
    ("001-542-415-0246x314", "+15424150246"),  # leading 001 country code removed
    ("7185624866", "+17185624866"),             # already-clean 10-digit number
    ("-6181", None),                            # too short -> null
    ("#ERROR!", None),                          # no digits at all -> null
    (None, None),                               # missing phone stays null
])
def test_clean_phone(raw, expected):
    """Test that clean_phone() returns the expected output."""
    out = apply_fn(clean_phone, "phone", [raw])
    assert out == [expected], f"{out}"

##---------------- schema tests: enriched & curated tables ----------------

EXPECTED_SCHEMAS = {
    "enriched.customers": [
        ("customer_id", "string"), ("customer_name", "string"), ("email", "string"),
        ("phone", "string"), ("address", "string"), ("segment", "string"),
        ("country", "string"), ("city", "string"), ("state", "string"),
        ("postal_code", "string"), ("region", "string"),
    ],
    "enriched.products": [
        ("product_id", "string"), ("category", "string"), ("sub_category", "string"),
        ("product_name", "string"), ("state", "string"), ("price_per_product", "decimal(10,2)"),
    ],
    "enriched.orders": [
        ("order_id", "string"), ("customer_id", "string"), ("product_id", "string"),
        ("order_date", "date"), ("ship_date", "date"), ("ship_mode", "string"),
        ("discount", "decimal(10,2)"), ("quantity", "int"),
        ("price", "decimal(10,2)"), ("profit", "decimal(10,2)"), ("row_id", "int"),
    ],
    "curated.orders": [
        ("order_id", "string"), ("customer_name", "string"), ("country", "string"),
        ("category", "string"), ("sub_category", "string"),
        ("order_year", "int"), ("profit", "decimal(10,2)"),
    ],
    "curated.profit_aggregates": [
        ("year", "int"), ("category", "string"), ("sub_category", "string"),
        ("customer_name", "string"), ("profit", "double"),
    ],
}

@pytest.mark.parametrize("table, expected_columns", list(EXPECTED_SCHEMAS.items()))
def test_table_schema(table, expected_columns):
    """Assert the table exists with exactly the expected columns, in order and type."""
    actual = [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]
    assert actual == expected_columns, f"{table}\n  expected: {expected_columns}\n  actual:   {actual}"

##---------------- duplicate-key tests: enriched tables ----------------

@pytest.mark.parametrize("table, key_cols", [
    ("enriched.customers", ["customer_id"]),
    ("enriched.products", ["product_id"]),
    ("enriched.orders", ["order_id", "product_id"]),
])
def test_no_duplicate_keys(table, key_cols):
    """Assert the table contains no duplicate rows for the given business key."""
    dupes = (
        spark.table(table)
        .groupBy(*key_cols)
        .count()
        .filter("count > 1")
        .limit(5)   # a few example duplicates are enough to fail loudly
        .collect()
    )
    assert not dupes, f"{table}: duplicate keys {key_cols} -> {dupes}"