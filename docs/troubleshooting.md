# Troubleshooting

## Network / Connection Issues

**Symptom:** Queries hang or fail with connection errors

DuckDB needs to download parquet files from Google Cloud Storage. If you're behind a firewall or have network issues:

1. Check your internet connection
2. Try the offline prefetch option:

   ```bash
   uv run python scripts/prefetch_data.py \
       --feed-type vehicle_positions \
       --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg \
       --start-date 2026-01-04 \
       --end-date 2026-01-04
   ```

## No Data Found

**Symptom:** Queries return empty results

The `start_date` and `end_date` in `dbt_project.yml` must match dates that have data available. To check available dates:

```bash
duckdb -c "
SELECT DISTINCT date
FROM read_parquet(
    'gs://parquet.gtfsrt.io/vehicle_positions/date=*/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg/data.parquet',
    hive_partitioning=true
)
ORDER BY date DESC
LIMIT 10;
"
```

Then update `dbt_project.yml` with valid dates.

## dbt Compilation Errors

**Symptom:** `dbt run` fails with compilation errors

Try these steps:

1. Clean and rebuild:

   ```bash
   uv run dbt clean
   uv run dbt run
   ```

2. Check you're in the right directory (should contain `dbt_project.yml`)

3. Ensure dependencies are installed:

   ```bash
   uv sync
   ```

## Getting Help

If you're still stuck, [open an issue](https://github.com/JarvusInnovations/gtfsrt-sandbox/issues) with:

- The full error message
- Your environment (Codespaces, local macOS/Linux/Windows)
- Steps to reproduce
