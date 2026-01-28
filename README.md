# GTFS-RT DuckDB Workshop

Query real-time transit data using DuckDB and dbt.

## Quick Start

### Option 1: GitHub Codespaces (Recommended)

1. Click the green "Code" button → "Open with Codespaces"
2. Wait for setup (~3 minutes, includes sample data download)
3. Run dbt:

   ```bash
   uv run dbt run
   ```

4. Query your data:

   ```bash
   duckdb workshop.duckdb -ui
   ```

### Option 2: Local Setup

**Prerequisites:**

- [uv](https://docs.astral.sh/uv/getting-started/installation/) - Python package manager
- [DuckDB CLI](https://duckdb.org/docs/installation/) - for interactive queries

```bash
# Clone the repo
git clone https://github.com/JarvusInnovations/gtfsrt-sandbox.git
cd gtfsrt-sandbox

# Install dependencies
uv sync && uv run dbt deps

# Download sample data (~30 seconds)
uv run python scripts/download_data.py --defaults

# Run dbt to create views
uv run dbt run

# Query the data
duckdb workshop.duckdb -ui
```

> **Note:** If you get a "Failed to download extension" error with `-ui`, see [DuckDB UI Extension Error](docs/troubleshooting.md#duckdb-ui-extension-error).

## How It Works

This workshop uses a two-phase approach:

1. **Download data** (`download_data.py`) - fetches parquet files to `data/`
2. **Transform data** (`dbt run`) - creates views in DuckDB reading from local files

This separation keeps dbt runs fast and makes the workflow easier to understand.

## Project Structure

```
gtfsrt-sandbox/
├── data/                        # Downloaded parquet data (gitignored)
│   ├── vehicle_positions/
│   ├── trip_updates/
│   └── service_alerts/
├── models/
│   ├── staging/                 # Views reading from data/
│   │   ├── stg_vehicle_positions.sql
│   │   ├── stg_trip_updates.sql
│   │   └── stg_service_alerts.sql
│   ├── intermediate/            # Transformations
│   └── marts/                   # Analytics views
└── scripts/
    └── download_data.py         # Data download script
```

## Downloading Different Data

### See what's available

```bash
uv run python scripts/download_data.py --list
```

### Download a different agency

```bash
uv run python scripts/download_data.py --agency septa --date 2026-01-20
```

### Use a different date

```bash
uv run python scripts/download_data.py --defaults --date 2026-01-20
```

See [docs/downloading_data.md](docs/downloading_data.md) for advanced options.

## Useful Commands

```bash
# Download sample data
uv run python scripts/download_data.py --defaults

# Run all models
uv run dbt run

# Run specific model
uv run dbt run --select stg_vehicle_positions

# Generate and view docs
uv run dbt docs generate && uv run dbt docs serve

# Query the database
duckdb workshop.duckdb -ui
```

## Data Schema

### vehicle_positions

| Column | Type | Description |
|--------|------|-------------|
| partition_date | date | Date partition |
| feed_base64 | string | Base64url-encoded feed URL |
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
| partition_date | date | Date partition |
| feed_base64 | string | Base64url-encoded feed URL |
| feed_timestamp | timestamp | When the feed was fetched |
| trip_id | string | Trip identifier |
| stop_id | string | Stop identifier |
| arrival_delay | int | Delay in seconds |
| departure_delay | int | Delay in seconds |

### service_alerts

| Column | Type | Description |
|--------|------|-------------|
| partition_date | date | Date partition |
| feed_base64 | string | Base64url-encoded feed URL |
| feed_timestamp | timestamp | When the feed was fetched |
| header_text | string | Alert title |
| description_text | string | Alert details |
| cause | int | Cause code |
| effect | int | Effect code |

## Need Help?

See [docs/troubleshooting.md](docs/troubleshooting.md) for common issues and solutions.

## License

Data sourced from public GTFS-RT feeds. Workshop materials are MIT licensed.
