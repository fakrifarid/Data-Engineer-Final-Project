# DAG and SQL files

## DAG

Berdasarkan `Project Flow Diagram`, kita membangun pipeline data:
- untuk mengekstrak file CSV dan mengubahnya menjadi file parquet
- untuk memuat file parquet ke Big Query: STAGING
- untuk mengubah data dalam STAGING ke model data DW dan memuat hasilnya ke Big Query: DW

## SQL

File SQL adalah kombinasi dari banyak tabel, menggabungkan data tersebut menjadi informasi yang berarti. Ada tiga model data: 
- `D_AIRPORT`
- `D_CITY_DEMO`
- `F_IMMIGRATION_DATA`.

## Airflow Variables

Tentukan variabel yang akan dimuat dalam file DAG.

```python
project_id = ""
dataproc_region = ""
dataproc_zone = ""
```
