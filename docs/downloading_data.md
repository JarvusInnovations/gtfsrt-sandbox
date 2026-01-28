# Downloading Data

This guide covers how to download GTFS-RT data for use with the dbt models.

## Quick Start

Download sample data with one command:

```bash
uv run python scripts/download_data.py --defaults
```

This downloads AC Transit data (vehicle positions, trip updates, and service alerts) for a known good date.

## How It Works

The script downloads parquet files from `http://parquet.gtfsrt.io` and saves them locally with a Hive-partitioned directory structure:

```
data/
├── vehicle_positions/
│   └── date=2026-01-24/
│       └── base64url=aHR0cHM6Ly.../
│           └── data.parquet
├── trip_updates/
│   └── date=2026-01-24/
│       └── base64url=aHR0cHM6Ly.../
│           └── data.parquet
└── service_alerts/
    └── date=2026-01-24/
        └── base64url=aHR0cHM6Ly.../
            └── data.parquet
```

The dbt models read all data from this directory, so you can download multiple feeds and dates.

## Usage Examples

### Download defaults for a specific date

```bash
uv run python scripts/download_data.py --defaults --date 2026-01-20
```

### Download a specific feed type

```bash
uv run python scripts/download_data.py \
    --feed-type vehicle_positions \
    --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \
    --start-date 2026-01-20 \
    --end-date 2026-01-24
```

### Download using base64-encoded feed URL

If you have a base64url from `seeds/available_feeds.csv`:

```bash
uv run python scripts/download_data.py \
    --feed-type vehicle_positions \
    --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg \
    --start-date 2026-01-01 \
    --end-date 2026-01-07
```

### Download from a different agency

1. View available feeds:

   ```bash
   duckdb -c "SELECT * FROM read_csv_auto('seeds/available_feeds.csv')"
   ```

2. Download the feed you want:

   ```bash
   uv run python scripts/download_data.py \
       --feed-type vehicle_positions \
       --feed-url "https://www3.septa.org/gtfsrt/septa-pa-us/Vehicle/rtVehiclePosition.pb" \
       --start-date 2026-01-20 \
       --end-date 2026-01-24
   ```

## Command Reference

| Option | Description |
|--------|-------------|
| `--defaults` | Download AC Transit data for all feed types |
| `--date DATE` | Single date for `--defaults` mode (default: 2026-01-24) |
| `--feed-type TYPE` | One of: `vehicle_positions`, `trip_updates`, `service_alerts` |
| `--feed-url URL` | Plain feed URL (will be base64url-encoded) |
| `--feed-base64 BASE64` | Base64url-encoded feed URL |
| `--start-date DATE` | Start date (YYYY-MM-DD) |
| `--end-date DATE` | End date (YYYY-MM-DD) |
| `--output-dir DIR` | Output directory (default: `data/`) |

## Available Feeds

The `seeds/available_feeds.csv` file lists known feeds with their base64url encodings:

| Agency | Feed Types Available |
|--------|---------------------|
| AC Transit | vehicle_positions, trip_updates, service_alerts |
| SEPTA | vehicle_positions, trip_updates, service_alerts |
| 511.org (SC, SF, etc.) | vehicle_positions, trip_updates, service_alerts |
| Metrolink | vehicle_positions |
| Big Blue Bus | vehicle_positions, trip_updates |

## Tips

- **Files are skipped if they exist** - safe to re-run the script
- **Multiple feeds can coexist** - download data from different agencies
- **Date range downloads** - use `--start-date` and `--end-date` for multiple days
- **Check data availability** - not all dates have data for all feeds

## Troubleshooting

### No data found for a date

Not all dates have data available. Try a different date or check what's available:

```bash
# Check available dates for a feed
curl -s "http://parquet.gtfsrt.io/vehicle_positions/" | grep "date="
```

### Download failed

Check your internet connection. The data source at `parquet.gtfsrt.io` should be publicly accessible without authentication.

### dbt models fail after download

Make sure the data directory structure is correct:

```bash
ls -la data/vehicle_positions/
```

You should see directories like `date=2026-01-24/`.
