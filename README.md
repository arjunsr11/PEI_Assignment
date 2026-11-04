# PEI Assignment: Data Pipeline

Medallion-style pipeline for the sample superstore dataset:

```
/Volumes/workspace/default/peiassignmentdata/          source files (Customer.xlsx, Products.csv, Orders.json)
    └─> raw.*          source data as-is (column names normalized only)
         └─> enriched.*   cleaned, typed, deduplicated tables
              └─> curated.*  analysis ready fact table + profit aggregates
```

Every load is a **one-time SCD Type-1 full overwrite** (`mode="overwrite"` + `overwriteSchema`) . The full list of loading assumptions is documented in the *DataLoadMethods* notebook, cell "Loading Assumptions".

## Assets in this folder

| Asset | Type | Purpose |
| --- | --- | --- |
| `DataLoadMethods` | notebook | Reference implementation of all pipeline methods. Each function is documented with docstrings; also documents the loading assumptions. Use it to develop/inspect the logic. |
| `DataLoadMethodsV2.py` | script | Python script version of the notebook `DataLoadMethods`. |
| `DataMainLoad` | notebook | The **pipeline runner**. Imports all the methods from `DataLoadMethods` notebooke and call the data load and transformation method 1 by 1. |
| `Test_DataLoadMethods` | notebook | Test notebook to check all key transformation functions, table schemas and data quality |
| `Test_DataLoadMethodsV2.py` | script | Python script version of the notebook `Test_DataLoadMethods`. |
| `Data Analysis & Quality Issues` | notebook | Exploratory analysis and data-quality investigation over the enriched/curated tables. |

## How to run

1. **Rebuild the pipeline** : open `DataMainLoad` and run all cells (raw → enriched → curated, with samples after each load).
2. **Validate** : open `Test_DataLoadMethods` and run all cells.
3. **Investigate data quality** : use the queries in `Data Analysis & Quality Issues`.

## Tables produced

* `raw.customers`, `raw.products`, `raw.orders`: source data as loaded
* `enriched.customers`, `enriched.products`, `enriched.orders`: cleaned, typed, deduplicated
* `curated.orders`: orders joined to customer (name, country) and product (category, sub_category), with `order_year` and `profit`
* `curated.profit_aggregates`: yearly profit per category / sub-category / customer

## Sources

Source files live in `/Volumes/workspace/default/peiassignmentdata/` (`Customer.xlsx`, `Products.csv`, `Orders.json`).