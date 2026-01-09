{{
  config(
    materialized='table'
  )
}}

/*
    Staging model for GTFS-RT service alerts.
    Downloads data from GCS on first run and caches locally.

    Denormalized: one row per informed_entity within each alert.
    Uses gs:// glob patterns with Hive partitioning for efficient access.
*/

SELECT
    -- Partition key (from Hive partitioning)
    date AS partition_date,

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
    _uuid
FROM {{ ref('base_service_alerts') }}
