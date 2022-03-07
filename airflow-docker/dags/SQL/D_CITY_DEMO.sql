CREATE OR REPLACE TABLE
  `{{ params.dwh_dataset }}.D_CITY_DEMO` AS
SELECT DISTINCT
  City CITY_NAME,
  State STATE_NAME,
  StateCode STATE_ID,
  MedianAge MEDIAN_AGE,
  MalePopulation MALE_POPULATION,
  FemalePopulation FEMALE_POPULATION,
  TotalPopulation TOTAL_POPULATION,
  NumberOfVeterans NUMBER_OF_VETERANS,
  ForeignBorn FOREIGN_BORN,
  AverageHouseholdSize AVERAGE_HOUSEHOLD_SIZE,
  AVG(
  IF
    (Race = 'White',
      COUNT,
      NULL)) WHITE_POPULATION,
  AVG(
  IF
    (Race = 'Black or African-American',
      COUNT,
      NULL)) BLACK_POPULATION,
  AVG(
  IF
    (Race = 'Asian',
      COUNT,
      NULL)) ASIAN_POPULATION,
  AVG(
  IF
    (Race = 'Hispanic or Latino',
      COUNT,
      NULL)) LATINO_POPULATION,
  AVG(
  IF
    (Race = 'American Indian and Alaska Native',
      COUNT,
      NULL)) NATIVE_POPULATION
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.demographic`
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10
