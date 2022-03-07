CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_STATE` AS
SELECT DISTINCT
  Code STATE_ID,
  State STATE_NAME
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.states`
