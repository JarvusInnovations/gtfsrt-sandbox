-- GTFS-RT Parquet Exploration Queries
-- Run these directly in DuckDB CLI: uv run duckdb
--
-- These queries demonstrate how to access GTFS-RT data from the public
-- GCS bucket using gs:// URLs with glob patterns and Hive partitioning.

-- ============================================================================
-- SETUP: Install and load required extensions
-- ============================================================================

INSTALL httpfs;
LOAD httpfs;

-- ============================================================================
-- EXAMPLE 1: Query a single feed with glob pattern (all dates)
-- ============================================================================

-- SEPTA Regional Rail vehicle positions - all available dates
SELECT
    date,
    COUNT(*) as records
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    hive_partitioning=true
)
GROUP BY date
ORDER BY date;

-- ============================================================================
-- EXAMPLE 2: Query with date filtering
-- ============================================================================

-- Vehicle positions for a specific date range
SELECT *
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    hive_partitioning=true
)
WHERE date >= '2026-01-04' AND date <= '2026-01-04'
LIMIT 10;

-- ============================================================================
-- EXAMPLE 3: Query ALL feeds for a date
-- ============================================================================

-- All vehicle position feeds for a single date
SELECT
    base64url,
    COUNT(*) as records,
    COUNT(DISTINCT vehicle_id) as unique_vehicles
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=2026-01-04/base64url=*/data.parquet',
    hive_partitioning=true
)
GROUP BY base64url
ORDER BY records DESC;

-- ============================================================================
-- EXAMPLE 4: Query everything (all dates, all feeds)
-- ============================================================================

-- WARNING: This may be slow/expensive for large date ranges!
SELECT
    date,
    base64url,
    COUNT(*) as records
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=*/data.parquet',
    hive_partitioning=true
)
GROUP BY date, base64url
ORDER BY date, records DESC;

-- ============================================================================
-- EXAMPLE 5: Daily summary statistics
-- ============================================================================

-- Get daily counts and unique vehicles per feed
SELECT
    date,
    COUNT(*) as total_records,
    COUNT(DISTINCT vehicle_id) as unique_vehicles,
    COUNT(DISTINCT route_id) as unique_routes,
    AVG(speed) FILTER (WHERE speed > 0) as avg_speed_mps
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    hive_partitioning=true
)
GROUP BY date
ORDER BY date;

-- ============================================================================
-- EXAMPLE 6: Trip updates with delay analysis
-- ============================================================================

-- Look at arrival delays for a specific feed
SELECT
    date,
    trip_id,
    stop_id,
    arrival_delay,
    to_timestamp(arrival_time) as arrival_time
FROM read_parquet(
    'gs://parquet.gtfsrt.io/trip_updates/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVHJpcC9ydFRyaXBVcGRhdGVzLnBi/data.parquet',
    hive_partitioning=true
)
WHERE date = '2026-01-04'
  AND arrival_delay IS NOT NULL
LIMIT 20;

-- ============================================================================
-- EXAMPLE 7: Service alerts
-- ============================================================================

-- Get active service alerts
SELECT
    date,
    entity_id,
    header_text,
    description_text,
    cause,
    effect
FROM read_parquet(
    'gs://parquet.gtfsrt.io/service_alerts/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvU2VydmljZS9ydFNlcnZpY2VBbGVydHMucGI/data.parquet',
    hive_partitioning=true
)
WHERE date = '2026-01-04'
LIMIT 10;

-- ============================================================================
-- EXAMPLE 8: Export to local parquet for faster subsequent queries
-- ============================================================================

-- Download and save locally (run once, query many times)
-- COPY (
--     SELECT *
--     FROM read_parquet(
--         'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
--         hive_partitioning=true
--     )
--     WHERE date = '2026-01-04'
-- ) TO 'local_vehicle_positions.parquet' (FORMAT PARQUET);

-- Then query locally:
-- SELECT * FROM read_parquet('local_vehicle_positions.parquet') LIMIT 10;
