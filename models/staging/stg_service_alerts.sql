/*
    Staging model for GTFS-RT service alerts.
    Reads pre-downloaded parquet data from the local data/ directory.

    Denormalized: one row per informed_entity within each alert.

    Before running: uv run python scripts/download_data.py --defaults
*/

SELECT
    -- Partition key (from Hive partitioning)
    date AS partition_date,

    -- Feed identifier (from Hive partitioning)
    base64url AS feed_base64,

    -- Source metadata
    feed_url,
    to_timestamp(feed_timestamp) AS feed_timestamp,
    entity_id,

    -- Alert fields
    cause,
    effect,
    severity_level,

    -- Active period
    to_timestamp(active_period_start) AS active_period_start,
    to_timestamp(active_period_end) AS active_period_end,

    -- Text content
    header_text,
    description_text,
    url,

    -- Informed entity
    agency_id,
    route_id,
    route_type,
    stop_id,
    trip_id,
    trip_route_id,
    trip_direction_id,

    -- generated ID
    uuid() as _uuid

FROM {{ read_gtfs_parquet('service_alerts') }}
