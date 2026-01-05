# GTFS-RT DuckDB Workshop

Query real-time transit data using DuckDB and dbt.

## Overview

This workshop demonstrates how to query GTFS Realtime parquet data from a public GCS bucket using DuckDB's httpfs extension and dbt for data transformation.

**Data source**: `gs://parquet.gtfsrt.io/` (also available at <http://parquet.gtfsrt.io/>)

Three feed types are available:

- **vehicle_positions** - Real-time vehicle locations
- **trip_updates** - Arrival/departure predictions
- **service_alerts** - Service disruption notices

## Quick Start

### Option 1: GitHub Codespaces (Recommended)

1. Click the green "Code" button → "Open with Codespaces"
2. Wait for the container to build (~2 minutes)
3. Run dbt:

   ```bash
   uv run dbt run
   ```

4. Query your data:

   ```bash
   uv run duckdb workshop.duckdb -ui
   ```

### Option 2: Local Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/gtfsrt-sandbox.git
cd gtfsrt-sandbox

# Install dependencies (requires uv: https://docs.astral.sh/uv/)
uv sync

# Run dbt to download and transform data
uv run dbt run

# Query the data
uv run duckdb workshop.duckdb -ui
```

## Choosing a Feed

Available feeds are listed in `seeds/available_feeds.csv`. To use a different feed:

```bash
# View available feeds
uv run duckdb -c "SELECT * FROM read_csv_auto('seeds/available_feeds.csv')"

# Run dbt with specific feeds (one variable per feed type)
uv run dbt run --vars '{
  "vehicle_positions_feed": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3ZlaGljbGVwb3NpdGlvbnM_YWdlbmN5PVND",
  "trip_updates_feed": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3RyaXB1cGRhdGVzP2FnZW5jeT1TQw",
  "service_alerts_feed": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3NlcnZpY2VhbGVydHM_YWdlbmN5PVND",
  "start_date": "2026-01-04",
  "end_date": "2026-01-04"
}'
```

### Feed Examples

| Agency | Feed Type | base64url |
|--------|-----------|-----------|
| SEPTA Regional Rail | vehicle_positions | `aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg` |
| 511.org SC | vehicle_positions | `aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3ZlaGljbGVwb3NpdGlvbnM_YWdlbmN5PVND` |
| AC Transit | vehicle_positions | `aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC92ZWhpY2xlcw` |
| Metrolink | vehicle_positions | `aHR0cHM6Ly9tZXRyb2xpbmstZ3Rmc3J0Lmdic2RpZ2l0YWwudXMvZmVlZC9ndGZzcnQtdmVoaWNsZXM` |

## Project Structure

```
gtfsrt-sandbox/
├── dbt_project.yml          # dbt configuration
├── profiles.yml             # DuckDB connection settings
├── models/
│   ├── staging/             # Data download & caching
│   │   ├── stg_vehicle_positions.sql
│   │   ├── stg_trip_updates.sql
│   │   └── stg_service_alerts.sql
│   └── marts/               # Analytics views
│       ├── feed_summary.sql
│       └── vehicle_activity.sql
├── macros/
│   └── read_gtfs_parquet.sql  # URL generation macro
├── seeds/
│   └── available_feeds.csv    # List of available feeds
└── scripts/
    ├── explore_feeds.sql      # Direct DuckDB queries
    ├── generate_feed_list.py  # Refresh feed list
    └── prefetch_data.py       # Pre-download for offline use
```

## How It Works

1. **Staging models** download parquet data from the public GCS bucket
2. Data is **cached locally** in `workshop.duckdb` as tables
3. **Mart models** are views that query the cached staging tables
4. Subsequent queries use **local data** (no repeated downloads)

To refresh data: `uv run dbt run --full-refresh`

## Direct DuckDB Queries

You can query the data directly without dbt using `gs://` URLs with glob patterns:

```sql
-- Start DuckDB CLI
uv run duckdb

-- Load httpfs extension
INSTALL httpfs;
LOAD httpfs;

-- Query with glob pattern (all dates for a feed)
SELECT date, COUNT(*) as records
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    hive_partitioning=true
)
GROUP BY date;

-- Query all feeds for a date
SELECT base64url, COUNT(*) as records
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=2026-01-04/base64url=*/data.parquet',
    hive_partitioning=true
)
GROUP BY base64url;
```

**Key advantage**: `gs://` URLs support glob patterns (`*`) for directory listing, while `http://` URLs do not.

See `scripts/explore_feeds.sql` for more examples.

## Offline Use

To pre-download data for offline use:

```bash
uv run python scripts/prefetch_data.py \
    --feed-type vehicle_positions \
    --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg \
    --start-date 2026-01-01 \
    --end-date 2026-01-07
```

Files are saved to `data/` with the same Hive partition structure.

## Useful Commands

```bash
# Run all models
uv run dbt run

# Run specific model
uv run dbt run --select stg_vehicle_positions

# Force re-download (full refresh)
uv run dbt run --full-refresh

# Load seed data
uv run dbt seed

# Generate docs
uv run dbt docs generate
uv run dbt docs serve

# Query the database
uv run duckdb workshop.duckdb
```

## Data Schema

### vehicle_positions

| Column | Type | Description |
|--------|------|-------------|
| partition_date | date | Date partition (from Hive partitioning) |
| feed_timestamp | timestamp | When the feed was fetched |
| vehicle_id | string | Vehicle identifier |
| trip_id | string | Trip identifier |
| route_id | string | Route identifier |
| latitude | float | Vehicle latitude |
| longitude | float | Vehicle longitude |
| speed | float | Speed in m/s |

### trip_updates

| Column | Type | Description |
|--------|------|-------------|
| partition_date | date | Date partition (from Hive partitioning) |
| feed_timestamp | timestamp | When the feed was fetched |
| trip_id | string | Trip identifier |
| stop_id | string | Stop identifier |
| arrival_delay | int | Delay in seconds |
| departure_delay | int | Delay in seconds |

### service_alerts

| Column | Type | Description |
|--------|------|-------------|
| partition_date | date | Date partition (from Hive partitioning) |
| feed_timestamp | timestamp | When the feed was fetched |
| header_text | string | Alert title |
| description_text | string | Alert details |
| cause | int | Cause code |
| effect | int | Effect code |

## License

Data sourced from public GTFS-RT feeds. Workshop materials are MIT licensed.
