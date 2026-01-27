/*
    Daily summary statistics for vehicle position data.
    Shows feed activity and coverage per day.
*/

SELECT
    feed_url,
    DATE(feed_timestamp) AS date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT vehicle_id) AS unique_vehicles,
    COUNT(DISTINCT route_id) AS unique_routes,
    COUNT(DISTINCT trip_id) AS unique_trips,
    MIN(feed_timestamp) AS first_update,
    MAX(feed_timestamp) AS last_update,
    AVG(speed) FILTER (WHERE speed IS NOT NULL AND speed > 0) AS avg_speed_mps,
    COUNT(*) FILTER (WHERE speed IS NOT NULL AND speed > 0) AS records_with_speed

FROM {{ ref('stg_vehicle_positions') }}
GROUP BY 1, 2
ORDER BY 2 DESC, 1
