-- Workaround for hugr-lab/mssql-ducklake#30.
--
-- mssql_ducklake 0.1.0 shapes ducklake_schema_versions with a primary key on
-- (begin_snapshot, schema_version). DuckLake writes one row per table in a
-- snapshot and table_id is what separates them, so any commit touching two or
-- more tables violates the key and the run fails after the retry budget.
--
-- Widening the key fixes it. The catalog shape is version stamped on
-- ducklake_metadata, so the extension does not put its own key back on later
-- attaches. Idempotent, and a no-op before the catalog exists.
--
-- Delete this file, its CI step and ensure_catalog_pk_fix() in the notebook once
-- the fix ships upstream.
IF OBJECT_ID('dbo.ducklake_schema_versions') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes i
               WHERE i.is_primary_key = 1
                 AND i.object_id = OBJECT_ID('dbo.ducklake_schema_versions')
                 AND (SELECT COUNT(*) FROM sys.index_columns ic
                      WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id) = 2)
BEGIN
    DELETE FROM dbo.ducklake_schema_versions WHERE table_id IS NULL;
    ALTER TABLE dbo.ducklake_schema_versions DROP CONSTRAINT pk_ducklake_schema_versions;
    ALTER TABLE dbo.ducklake_schema_versions ALTER COLUMN table_id BIGINT NOT NULL;
    ALTER TABLE dbo.ducklake_schema_versions ADD CONSTRAINT pk_ducklake_schema_versions
        PRIMARY KEY (begin_snapshot, schema_version, table_id);
END
