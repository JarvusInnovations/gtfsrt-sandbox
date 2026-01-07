{{
  config(
    materialized='table'
  )
}}

/*
    Staging model for GTFS-RT vehicle positions.
    Downloads data from GCS on first run and caches locally.

    One row per vehicle position update in the feed.
    Uses gs:// glob patterns with Hive partitioning for efficient access.
*/

SELECT
    -- Partition key (from Hive partitioning)
    date AS partition_date,

    -- Source metadata
    feed_url,
    to_timestamp(feed_timestamp) AS feed_timestamp,
    entity_id,

    -- Trip descriptor
    trip_id,
    route_id,
    direction_id,
    start_time,
    start_date,
    schedule_relationship,

    -- Vehicle descriptor
    vehicle_id,
    vehicle_label,
    license_plate,

    -- Position
    latitude,
    longitude,
    bearing,
    odometer,
    speed,

    -- Status
    current_stop_sequence,
    stop_id,
    current_status,
    to_timestamp(timestamp) AS vehicle_timestamp,
    congestion_level,
    occupancy_status,
    occupancy_percentage

FROM {{ ref('base_vehicle_positions') }}

