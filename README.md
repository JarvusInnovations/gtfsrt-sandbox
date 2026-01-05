# GTFS-RT DuckDB Workshop

Query real-time transit data using DuckDB and dbt.

## Overview

This workshop demonstrates how to query GTFS Realtime parquet data from a public GCS bucket using DuckDB's httpfs extension and dbt for data transformation.

**Data source**: `http://parquet.gtfsrt.io/`

## Quick Start

```bash
# Install dependencies
uv sync

# Pick a feed and run dbt
uv run dbt run --vars '{"feed_base64": "YOUR_FEED_BASE64", "start_date": "2026-01-01", "end_date": "2026-01-07"}'

# Query the data
uv run duckdb workshop.duckdb
```

## Available Feeds

See `seeds/available_feeds.csv` for a list of available feeds and their base64-encoded URLs.

## More Documentation

Full workshop instructions coming soon.
