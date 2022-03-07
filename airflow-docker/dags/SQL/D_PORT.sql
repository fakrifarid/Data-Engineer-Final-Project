CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_PORT` AS
SELECT DISTINCT
  ID PORT_ID,
  Port PORT_NAME
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.ports`
