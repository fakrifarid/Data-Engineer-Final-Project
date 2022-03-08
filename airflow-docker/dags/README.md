# DAG and SQL files

## DAG

Berdasarkan `Project Flow Diagram`, kita membangun pipeline data:
- untuk mengekstrak file CSV dan mengubahnya menjadi file parquet
- untuk memuat file parquet ke Big Query: STAGING
- untuk mengubah data dalam STAGING ke model data DW dan memuat hasilnya ke Big Query: DW


## Airflow Variables

Tentukan variabel yang akan dimuat dalam file DAG.

```python
project_id = "agile-genius-342013"
dataproc_region = "us-central1"
dataproc_zone = "us-central1-c"
```
