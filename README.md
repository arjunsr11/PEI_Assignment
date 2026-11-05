# PEI Assignment: Data Pipeline

Medallion-style pipeline for the sample superstore dataset:

```
/Volumes/workspace/default/peiassignmentdata/          source files (Customer.xlsx, Products.csv, Orders.json)
    └─> raw.*          source data as-is (column names normalized only)
         └─> enriched.*   cleaned, typed, deduplicated tables
              └─> curated.*  analysis ready fact table + profit aggregates
```

Every load is a **one-time SCD Type-1 full overwrite** (`mode="overwrite"` + `overwriteSchema`).

## Folder structure

```
/PEI Assignment
├── README.md                        this file
├── src/                             the pipeline, implemented with the notebook method
│   ├── 01_raw_data_ingestion        Task 1 : load the three source files into the raw schema
│   ├── 02_data_enrichment           Tasks 2-3 : clean customers/products; clean + dedup orders into enriched.*
│   ├── 03_aggregations              Task 3 master table + Task 4 : curated.orders and curated.profit_aggregates
│   └── utils/                       the shared layer libraries
│       ├── 01_raw_utils             Task 1 library : raw loads (documents the loading assumptions)
│       ├── 02_enriched_utils        Tasks 2-3 library : cleaning and enrichment functions
│       └── 03_curated_utils         Tasks 3-4 library : curated join and profit aggregates
├── sql_queries/                      Task 5 : one SQL file per required aggregate
│   ├── 01_profit_by_year.sql
│   ├── 02_profit_by_year_and_category.sql
│   ├── 03_profit_by_customer.sql
│   └── 04_profit_by_customer_and_year.sql
└── tests/                            pytest suite : tests the actual notebook implementation
    ├── conftest.py                   loads the real notebook code so tests never run against a copy
    ├── test_raw_ingestion.py         Task 1 tests
    ├── test_enrichment.py            Tasks 2-3 tests
    └── test_aggregation.py           Tasks 4-5 tests
```

## How to run

1. **Rebuild the pipeline**: run the notebooks in `src/` in order : `01_raw_data_ingestion` → `02_data_enrichment` → `03_aggregations`. Each imports its stage's functions from the matching layer library (`utils/01_raw_utils` / `utils/02_enriched_utils` / `utils/03_curated_utils`) via `%run`.
2. **Run the tests**: run the pytest suite from any Python notebook cell. The output reports the number of test cases passed and failed : last verified: **34 passed, 0 failed**.

## Tests

The suite in `tests/` validates each task against the **actual implementation**: `conftest.py` reads the layer library notebooks' (`01_raw_utils`, `02_enriched_utils`, `03_curated_utils` in `src/utils/`) code cells and executes them, so the tests always exercise the real pipeline code.

| File | Task | What it verifies |
| --- | --- | --- |
| `test_raw_ingestion.py` | 1 | Re-runs each loader and compares the raw tables with independent re-reads of the source files : row to row equality using checksum, normalized column names, raw values preserved as-is |
| `test_enrichment.py` | 2-3 | `clean_name`/`clean_phone` on known dirty inputs; day-first date parsing; duplicate order lines collapse to their latest version; numeric casts to decimal/int; enriched tables cleaned and deduplicated; curated master table joins customer + product attributes and keeps unmatched orders (left joins) |
| `test_aggregation.py` | 4-5 | `curated.profit_aggregates` equals an independent recomputation of the same grain; totals reconcile with `curated.orders`; profit stored rounded to 2 decimals; known-fixture group sums; every Task 5 SQL file reproduces the expected totals |

## Tables produced

* `raw.customers`, `raw.products`, `raw.orders`: source data as loaded
* `enriched.customers`, `enriched.products`, `enriched.orders`: cleaned, typed, deduplicated
* `curated.orders`: orders joined to customer (name, country) and product (category, sub_category), with `order_year` and `profit`
* `curated.profit_aggregates`: profit by year / category / sub-category / customer

## Sources

Source files live in `/Volumes/workspace/default/peiassignmentdata/` (`Customer.xlsx`, `Products.csv`, `Orders.json`).