/*
    Staging model for GTFS-RT vehicle positions.
    Reads pre-downloaded parquet data from the local data/ directory.

    One row per vehicle position update in the feed.

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
    occupancy_percentage,

    -- generated ID
    uuid() as _uuid

FROM read_parquet('data/vehicle_positions/date=*/base64url=*/data.parquet', hive_partitioning=true)
