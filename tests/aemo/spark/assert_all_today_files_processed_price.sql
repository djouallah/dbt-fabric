-- Every landed price_today file must appear in fct_price_today. Rows returned = files the
-- downloader recorded but the model never ingested.
SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'price_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT `file`
    FROM {{ ref('fct_price_today') }}
  )
