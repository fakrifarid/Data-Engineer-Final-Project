#transformation.py
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()


#convert and write csv to parquet
df_temp = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/GlobalLandTemperaturesByCity.csv")
df_temp.write.mode("Overwrite").parquet("gs://file_final_project/raw/globaltempbycity.parquet")

df_airport = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/airport-codes_csv.csv")
df_airport.write.mode("Overwrite").parquet("gs://file_final_project/raw/airportcodes.parquet")

df_immigration = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/immigration_data_sample.csv")
df_immigration.write.mode("Overwrite").parquet("gs://file_final_project/raw/immigration.parquet")

df_port = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/USPORT.csv")
df_port.write.mode("Overwrite").parquet("gs://file_final_project/raw/usport.parquet")

df_state = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/USSTATE.csv")
df_state.write.mode("Overwrite").parquet("gs://file_final_project/raw/usstate.parquet")

df_country = spark.read.options(header='true', inferSchema='true').csv("gs://file_final_project/Input/USCOUNTRY.csv")
df_country.write.mode("Overwrite").parquet("gs://file_final_project/raw/uscountry.parquet")

# The delimiter for this file (us-cities-demographics.csv) is semicolon (;)
# We have to change the default delimiter
df_demographic = spark.read.options(header="true", inferSchema="true", delimiter=";").csv("gs://file_final_project/Input/us-cities-demographics.csv")
df_demographic.printSchema()
# There is a problem in Header of this df, 
# Therefore, we need to rename the header before write it into parquet file
# Header: City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count

# Rename all column names with new column names
demographic_new_col_names = ["City", "State", "MedianAge", "MalePopulation", "FemalePopulation", "TotalPopulation", "NumberOfVeterans", "ForeignBorn", "AverageHouseholdSize", "StateCode", "Race", "Count"]
df_demographic = df_demographic.toDF(*demographic_new_col_names)

# Then, we convert it into parquet
df_demographic.write.mode("Overwrite").parquet("gs://file_final_project/raw/demographic.parquet")

