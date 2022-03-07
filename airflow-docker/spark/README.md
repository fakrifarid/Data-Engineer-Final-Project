# Spark Files

Untuk membangun data pipeline, ada beberapa proses yang diformulasikan sebagai ETL, yaitu Extract, Transform, dan Load. Dalam proses Extract, kita harus mengekstrak file CSV dan mengubah file tersebut menjadi file parket.

```python
# extract_to_parquet.py

# import library
from pyspark.sql import SparkSession

# create session for Spark
spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()
```

Potongan kode di atas mendefinisikan library yang kita gunakan dan membuat Spark Session. Kemudian, langkah selanjutnya adalah membaca file CSV yang telah kita upload ke Cloud Storage dengan nama bucket `data_final_project` dan folder `INPUT`. File yang dikonversi (file parket) disimpan ke dalam folder `raw`.

```python

#convert and write csv to parquet
df_temp = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/GlobalLandTemperaturesByCity.csv")
df_temp.write.mode("Overwrite").parquet("gs://data_final_project/raw/globaltempbycity.parquet")

df_airport = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/airport-codes_csv.csv")
df_airport.write.mode("Overwrite").parquet("gs://data_final_project/raw/airportcodes.parquet")

df_imigration = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/immigration_data_sample.csv")
df_imigration.write.mode("Overwrite").parquet("gs://data_final_project/raw/imigration.parquet")

df_demographic = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/us-cities-demographics.csv")
df_demographic.write.mode("Overwrite").parquet("gs://data_final_project/raw/demographic.parquet")

df_port = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/USPORT.csv")
df_port.write.mode("Overwrite").parquet("gs://data_final_project/raw/usport.parquet")

df_state = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/USSTATE.csv")
df_state.write.mode("Overwrite").parquet("gs://data_final_project/raw/usstate.parquet")

df_country = spark.read.options(header='true', inferSchema='true').csv("gs://data_final_project/INPUT/USCOUNTRY.csv")
df_country.write.mode("Overwrite").parquet("gs://data_final_project/raw/uscountry.parquet")

```
