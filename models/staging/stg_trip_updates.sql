{{
  config(
    materialized='table'
  )
}}

/*
    Staging model for GTFS-RT trip updates.
    Downloads data from GCS on first run and caches locally.

    Denormalized: one row per stop_time_update within each trip update.
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

    -- Trip-level fields
    to_timestamp(trip_timestamp) AS trip_timestamp,
    trip_delay,

    -- Stop time update fields
    stop_sequence,
    stop_id,
    arrival_delay,
    to_timestamp(arrival_time) AS arrival_time,
    arrival_uncertainty,
    departure_delay,
    to_timestamp(departure_time) AS departure_time,
    departure_uncertainty,
    stop_schedule_relationship,

    -- generated ID
    _uuid

FROM {{ ref('base_trip_updates') }}
