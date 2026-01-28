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

## List Available Agencies

See what data is available:

```bash
uv run python scripts/download_data.py --list
```

This shows all agencies with their feed counts and available date ranges.

## Download by Agency

Download all feeds for a specific agency:

```bash
# Download SEPTA data (all systems)
uv run python scripts/download_data.py --agency septa --date 2026-01-20

# Download AC Transit (same as --defaults)
uv run python scripts/download_data.py --agency actransit
```

The script shows estimated download sizes before downloading.

## Multi-System Agencies

Some agencies like SEPTA have multiple transit systems (bus, rail) with separate feeds.

### View system breakdown

```bash
uv run python scripts/download_data.py --list
```

Output shows systems for multi-system agencies:

```
  septa                  SEPTA                            6      2026-01-01 to 2026-01-25
    └─ septa/bus         Bus                              3      2026-01-01 to 2026-01-25
    └─ septa/rail        Regional Rail                    3      2026-01-01 to 2026-01-25
```

### Download a specific system

```bash
# Just SEPTA bus data
uv run python scripts/download_data.py --agency septa/bus --date 2026-01-20

# Just SEPTA rail data
uv run python scripts/download_data.py --agency septa/rail --date 2026-01-20
```

## Usage Examples

### Download defaults for a specific date

```bash
uv run python scripts/download_data.py --defaults --date 2026-01-20
```

### Download a specific feed type (advanced)

```bash
uv run python scripts/download_data.py \
    --feed-type vehicle_positions \
    --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \
    --start-date 2026-01-20 \
    --end-date 2026-01-24
```

### Download using base64-encoded feed URL (advanced)

```bash
uv run python scripts/download_data.py \
    --feed-type vehicle_positions \
    --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg \
    --start-date 2026-01-01 \
    --end-date 2026-01-07
```

## Command Reference

| Option | Description |
|--------|-------------|
| `--list` | List available agencies from inventory |
| `--defaults` | Download AC Transit data for all feed types |
| `--agency AGENCY` | Download feeds for an agency (e.g., `septa`) or agency/system (e.g., `septa/bus`) |
| `--date DATE` | Date for `--defaults`/`--agency` mode (default: 2026-01-24) |
| `--feed-type TYPE` | One of: `vehicle_positions`, `trip_updates`, `service_alerts` (advanced) |
| `--feed-url URL` | Plain feed URL (advanced) |
| `--feed-base64 BASE64` | Base64url-encoded feed URL (advanced) |
| `--start-date DATE` | Start date for date range (advanced) |
| `--end-date DATE` | End date for date range (advanced) |
| `--output-dir DIR` | Output directory (default: `data/`) |

## Tips

- **Files are skipped if they exist** - safe to re-run the script
- **Multiple feeds can coexist** - download data from different agencies
- **Date range downloads** - use `--start-date` and `--end-date` for multiple days
- **Check data availability** - not all dates have data for all feeds

## Troubleshooting

### No data found for a date

Not all dates have data available. Try a different date or use `--list` to see available date ranges.

### Download failed

Check your internet connection. The data source at `parquet.gtfsrt.io` should be publicly accessible without authentication.

### dbt models fail after download

Make sure the data directory structure is correct:

```bash
ls -la data/vehicle_positions/
```

You should see directories like `date=2026-01-24/`.
