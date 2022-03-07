# DAG and SQL files

## DAG

Based on the `Project Flow Diagram`, we build data pipeline:
- to extract CSV files and convert into parquet files
- to load the parquet files to Big Query: STAGING
- to transform data in STAGING to DW datamodels and load the result to Big Query: DW

## SQL

The SQL files are combination of many tables, aggregating those data into meaningful information. There are three datamodels: `D_AIRPORT`, `D_CITY_DEMO`, and `F_IMMIGRATION_DATA`.

## Airflow Variables

Define the variables which will be loaded in DAG file.

```python
project_id = ""
dataproc_region = ""
dataproc_zone = ""
```