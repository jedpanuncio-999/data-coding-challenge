{{ config(materialized='table') }}

-- Sanity check: verify data was loaded by the ingestion pipeline.
-- All counts should be > 0 after running your Dagster assets.

SELECT
    'publishers' AS entity,
    toFloat64(count(*)) AS record_count
FROM raw.publishers

UNION ALL

SELECT
    'campaigns' AS entity,
    toFloat64(count(*)) AS record_count
FROM raw.campaigns

UNION ALL

SELECT
    'ad_units' AS entity,
    toFloat64(count(*)) AS record_count
FROM raw.ad_units

UNION ALL

SELECT
    'ad_events' AS entity,
    toFloat64(count(*)) AS record_count
FROM raw.ad_events
