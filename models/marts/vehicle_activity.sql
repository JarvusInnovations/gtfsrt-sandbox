{{
  config(
    materialized='view'
  )
}}

/*
    Hourly vehicle activity analysis.
    Shows vehicle movement patterns and coverage over time.
*/

SELECT
    vehicle_id,
    vehicle_label,
    route_id,
    DATE_TRUNC('hour', feed_timestamp) AS hour,

    -- Activity metrics
    COUNT(*) AS updates_per_hour,
    COUNT(DISTINCT trip_id) AS trips_served,

    -- Speed analysis
    AVG(speed) FILTER (WHERE speed IS NOT NULL AND speed > 0) AS avg_speed_mps,
    MAX(speed) FILTER (WHERE speed IS NOT NULL) AS max_speed_mps,

    -- Geographic coverage (bounding box)
    MIN(latitude) AS min_lat,
    MAX(latitude) AS max_lat,
    MIN(longitude) AS min_lon,
    MAX(longitude) AS max_lon,

    -- Time range
    MIN(feed_timestamp) AS first_update,
    MAX(feed_timestamp) AS last_update

FROM {{ ref('stg_vehicle_positions') }}
WHERE vehicle_id IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 4 DESC, 1
