-- DUID dimension (Spark). An incremental merge on DUID, as on dwh: a unit that leaves the
-- reference CSVs keeps its row, because fct_summary still holds its history, and a changed
-- attribute is updated in place. MAX() picks one value per unit, as on dwh; first() would
-- pick whichever row Spark read first, and could change from run to run.
--
-- The reference CSVs are headered. An incremental model cannot read a csv temp view
-- (macros/spark_read_csv.sql), so the pre_hooks read each file through one and stage the
-- columns this model uses into a Delta table, under names Delta accepts; the post_hooks
-- drop the stages.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='DUID',
    pre_hook=[
      "{{ spark_reference_view('duid_data') }}",
      "{{ spark_reference_stage('duid_data', 'DUID, Region, `Fuel Source - Descriptor` AS FuelSourceDescriptor, Participant') }}",
      "{{ spark_reference_view('facilities') }}",
      "{{ spark_reference_stage('facilities', '`Facility Code` AS DUID, `Participant Name` AS Participant') }}",
      "{{ spark_reference_view('WA_ENERGY') }}",
      "{{ spark_reference_stage('WA_ENERGY', 'DUID, Technology') }}",
      "{{ spark_reference_view('geo_data') }}",
      "{{ spark_reference_stage('geo_data', 'DUID, latitude, longitude') }}"
    ],
    post_hook=[
      "{{ spark_drop_reference_stage('duid_data') }}",
      "{{ spark_drop_reference_stage('facilities') }}",
      "{{ spark_drop_reference_stage('WA_ENERGY') }}",
      "{{ spark_drop_reference_stage('geo_data') }}"
    ]
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

WITH states AS (
    SELECT 'WA1' AS RegionID, 'Western Australia' AS State
    UNION ALL SELECT 'QLD1', 'Queensland'
    UNION ALL SELECT 'NSW1', 'New South Wales'
    UNION ALL SELECT 'TAS1', 'Tasmania'
    UNION ALL SELECT 'SA1', 'South Australia'
    UNION ALL SELECT 'VIC1', 'Victoria'
),

duid_aemo AS (
    SELECT
        DUID,
        MAX(Region) AS Region,
        MAX(FuelSourceDescriptor) AS FuelSourceDescriptor,
        MAX(Participant) AS Participant
    FROM {{ spark_reference_relation('duid_data') }}
    WHERE length(DUID) > 2
    GROUP BY DUID
),

duid_wa AS (
    SELECT
        f.DUID,
        'WA1' AS Region,
        w.Technology AS FuelSourceDescriptor,
        f.Participant
    FROM {{ spark_reference_relation('facilities') }} f
    LEFT JOIN {{ spark_reference_relation('WA_ENERGY') }} w ON f.DUID = w.DUID
),

duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
),

geo AS (
    SELECT
        DUID,
        MAX(CAST(latitude AS DOUBLE)) AS latitude,
        MAX(CAST(longitude AS DOUBLE)) AS longitude
    FROM {{ spark_reference_relation('geo_data') }}
    WHERE latitude IS NOT NULL
    GROUP BY DUID
)

SELECT
    a.DUID,
    MAX(a.Region) AS Region,
    MAX(concat(upper(substring(trim(a.FuelSourceDescriptor), 1, 1)), lower(substring(trim(a.FuelSourceDescriptor), 2)))) AS FuelSourceDescriptor,
    MAX(a.Participant) AS Participant,
    MAX(states.State) AS State,
    MAX(geo.latitude) AS latitude,
    MAX(geo.longitude) AS longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.DUID = geo.DUID
GROUP BY a.DUID
