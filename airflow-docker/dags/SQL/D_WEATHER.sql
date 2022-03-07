CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_WEATHER` AS
SELECT DISTINCT
  SUBSTR(dt, 0, 4) YEAR,
  City CITY,
  Country COUNTRY,
  Latitude LATITUDE,
  Longitude LONGITUDE,
  AverageTemperature AVG_TEMPERATURE
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.weathers`
WHERE Country = 'United States'
