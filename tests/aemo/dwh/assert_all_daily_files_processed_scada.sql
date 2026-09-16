{{ config(tags=['heavy']) }}
-- Every landed daily file must appear in fct_scada. Rows returned = files the
-- downloader recorded but the model never ingested.
SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'daily'
  AND csv_filename NOT IN (
    SELECT DISTINCT [file]
    FROM {{ ref('fct_scada') }}
  )
