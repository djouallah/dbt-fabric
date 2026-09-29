-- View over the archive log download_aemo.py writes to the landing lakehouse
-- (csv_raw_archive_log.parquet), read through the shortcut with OPENROWSET. The name is the
-- same on every engine, so every ref('stg_csv_archive_log') (models, tests) is shared.
{{ config(materialized='view', schema='landing') }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    csv_filename
FROM OPENROWSET(
    BULK '{{ get_root_path() }}/csv_raw_archive_log.parquet',
    FORMAT = 'PARQUET'
) AS log
