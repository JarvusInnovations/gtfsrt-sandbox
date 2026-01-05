-- GTFS-RT Parquet Exploration Queries
-- Run these directly in DuckDB CLI: uv run duckdb
--
-- These queries demonstrate how to access GTFS-RT data from the public
-- bucket without using dbt. Useful for exploration and ad-hoc analysis.

-- ============================================================================
-- SETUP: Install and load required extensions
-- ============================================================================

INSTALL httpfs;
LOAD httpfs;

-- ============================================================================
-- EXAMPLE 1: Query a single day of vehicle positions
-- ============================================================================

-- SEPTA Regional Rail vehicle positions for one day
SELECT *
FROM read_parquet(
    'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet'
)
LIMIT 10;

-- ============================================================================
-- EXAMPLE 2: Query multiple days using a list of URLs
-- ============================================================================

-- Vehicle positions for a week (manually specified URLs)
SELECT
    feed_url,
    to_timestamp(feed_timestamp) as feed_ts,
    vehicle_id,
    latitude,
    longitude,
    speed
FROM read_parquet([
    'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-02/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-03/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet'
], union_by_name=true)
WHERE vehicle_id IS NOT NULL
LIMIT 100;

-- ============================================================================
-- EXAMPLE 3: Dynamic date range using generate_series
-- ============================================================================

-- Generate URLs dynamically for a date range
-- This uses DuckDB's list_transform to build URLs

WITH date_urls AS (
    SELECT list_transform(
        generate_series(DATE '2026-01-01', DATE '2026-01-03', INTERVAL 1 DAY),
        d -> 'http://parquet.gtfsrt.io/vehicle_positions/date=' ||
             strftime(d, '%Y-%m-%d') ||
             '/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet'
    ) as urls
)
SELECT COUNT(*) as total_records
FROM read_parquet((SELECT urls FROM date_urls), union_by_name=true);

-- ============================================================================
-- EXAMPLE 4: Daily summary statistics
-- ============================================================================

-- Get daily counts and unique vehicles per feed
SELECT
    DATE(to_timestamp(feed_timestamp)) as date,
    COUNT(*) as total_records,
    COUNT(DISTINCT vehicle_id) as unique_vehicles,
    COUNT(DISTINCT route_id) as unique_routes,
    AVG(speed) FILTER (WHERE speed > 0) as avg_speed_mps
FROM read_parquet(
    'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet'
)
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- EXAMPLE 5: Trip updates with delay analysis
-- ============================================================================

-- Look at arrival delays for a specific feed
SELECT
    trip_id,
    stop_id,
    arrival_delay,
    to_timestamp(arrival_time) as arrival_time,
    departure_delay,
    to_timestamp(departure_time) as departure_time
FROM read_parquet(
    'http://parquet.gtfsrt.io/trip_updates/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVHJpcC9ydFRyaXBVcGRhdGVzLnBi/data.parquet'
)
WHERE arrival_delay IS NOT NULL
LIMIT 20;

-- ============================================================================
-- EXAMPLE 6: Service alerts
-- ============================================================================

-- Get active service alerts
SELECT
    entity_id,
    header_text,
    description_text,
    cause,
    effect,
    to_timestamp(active_period_start) as start_time,
    to_timestamp(active_period_end) as end_time
FROM read_parquet(
    'http://parquet.gtfsrt.io/service_alerts/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvU2VydmljZS9ydFNlcnZpY2VBbGVydHMucGI/data.parquet'
)
LIMIT 10;

-- ============================================================================
-- EXAMPLE 7: Export to local parquet for faster subsequent queries
-- ============================================================================

-- Download and save locally (run once, query many times)
-- COPY (
--     SELECT *
--     FROM read_parquet(
--         'http://parquet.gtfsrt.io/vehicle_positions/date=2026-01-01/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet'
--     )
-- ) TO 'local_vehicle_positions.parquet' (FORMAT PARQUET);

-- Then query locally:
-- SELECT * FROM read_parquet('local_vehicle_positions.parquet') LIMIT 10;
