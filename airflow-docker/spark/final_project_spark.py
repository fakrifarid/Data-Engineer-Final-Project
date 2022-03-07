#transformation.py
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()


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


#agregare 
#df.createOrReplaceTempView("sales")
#highestPriceUnitDF = spark.sql("select * from sales where UnitPrice >= 3.0")
#highestPriceUnitDF.write.mode("Overwrite").parquet("gs://hive-example/final-project/output/highest_prices.parquet")