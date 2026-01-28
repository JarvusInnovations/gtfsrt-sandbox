{{
    config(
        materialized='table'
    )
}}

/*
    Staging model for parquet.gtfsrt.io feed inventory.
    Reads directly from the public inventory JSON file.

    This provides metadata about all available GTFS-RT feeds
    including agency info, feed types, date ranges, and sizes.
*/

SELECT
    url AS feed_url,
    base64url AS feed_base64,
    agency_id,
    agency_name,
    feed_type,
    date_min,
    date_max,
    total_records,
    total_bytes,

    -- Computed fields
    date_max::date - date_min::date + 1 AS days_available,
    total_bytes / NULLIF(date_max::date - date_min::date + 1, 0) AS avg_bytes_per_day

FROM read_json_auto('gs://parquet.gtfsrt.io/inventory.json')
