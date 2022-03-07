# Spark Files

Untuk membangun data pipeline, ada beberapa proses yang diformulasikan sebagai ETL, yaitu Extract, Transform, dan Load. Dalam proses Extract, kita harus mengekstrak file CSV dan mengubah file tersebut menjadi file parket.

```python
# extract_to_parquet.py

# import library
from pyspark.sql import SparkSession

# create session for Spark
spark = SparkSession \
    .builder \
    .appName("Transforming INPUT files into PARQUET files") \
    .getOrCreate()
```

The pieces of code above is defining the library that we use and creating Spark Session. Then, the next step is reading CSV files that we have uploaded into Cloud Storage with bitbucket name `final_project_ekoteguh` and folder `INPUT`. The converted files (parquet files) are stored into `RAW` folder.

```python
# extract_to_parquet.py

df_country = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USCOUNTRY.csv")
df_country.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/uscountry.parquet")

df_port = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USPORT.csv")
df_port.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/usport.parquet")

df_state = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/USSTATE.csv")
df_state.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/usstate.parquet")

df_airport = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/airport-codes_csv.csv")
df_airport.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/airportcodes.parquet")

df_temp = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/GlobalLandTemperaturesByCity.csv")
df_temp.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/globaltempbycity.parquet")

df_immigration = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/immigration_data_sample.csv")
df_immigration.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/imigration.parquet")

df_demographic = spark.read.options(header="true", inferSchema="true").csv("gs://final_project_ekoteguh/INPUT/us-cities-demographics.csv")
df_demographic.write.mode("Overwrite").parquet("gs://final_project_ekoteguh/RAW/demographic.parquet")
```

## Additional information

After I tried five times, similar problems occured: `Column name of df_immigration`. Here is the column name:

```csv
City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count
```

Some columns has space and hypen character, therefore, we need to rename the column name:

```python
df_demographic.withColumnRenamed("Median Age", "MedianAge") \
    .withColumnRenamed("Male Population", "MalePopulation") \
    .withColumnRenamed("Female Population", "FemalePopulation") \
    .withColumnRenamed("Total Population", "TotalPopulation") \
    .withColumnRenamed("Number of Veterans", "NumberofVeterans") \
    .withColumnRenamed("Foreign-born", "ForeignBorn") \
    .withColumnRenamed("Average Household Size", "AverageHouseholdSize") \
    .withColumnRenamed("State Code", "StateCode")
```
