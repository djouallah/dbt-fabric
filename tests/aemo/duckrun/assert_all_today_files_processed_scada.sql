{{ config(tags=['heavy']) }}
-- Every landed scada_today file must appear in fct_scada_today. Rows returned = files the
-- downloader recorded but the model never ingested.
SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'scada_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT file
    FROM {{ ref('fct_scada_today') }}
  )
