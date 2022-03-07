CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_COUNTRY` AS
SELECT DISTINCT
  Code COUNTRY_ID,
  I94CTRY COUNTRY_NAME
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.countries`
