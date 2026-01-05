#!/usr/bin/env python3
"""
Pre-download parquet files for offline workshop use.

This script downloads parquet files from the public GCS bucket to a local
directory, allowing dbt models to query local files instead of remote URLs.

Usage:
    # Download vehicle_positions for a specific feed and date range
    uv run python scripts/prefetch_data.py \
        --feed-type vehicle_positions \
        --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9ndGZzcnQvc2VwdGEtcGEtdXMvVmVoaWNsZS9ydFZlaGljbGVQb3NpdGlvbi5wYg \
        --start-date 2026-01-01 \
        --end-date 2026-01-07

    # Or use the feed URL directly (will be encoded)
    uv run python scripts/prefetch_data.py \
        --feed-type vehicle_positions \
        --feed-url "https://www3.septa.org/gtfsrt/septa-pa-us/Vehicle/rtVehiclePosition.pb" \
        --start-date 2026-01-01 \
        --end-date 2026-01-07

After downloading, update your dbt macro or models to read from local files.
"""

import argparse
import base64
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import HTTPError


BASE_URL = "http://parquet.gtfsrt.io"
LOCAL_DATA_DIR = Path("data")


def encode_base64url(url: str) -> str:
    """Encode URL to base64url (without padding)."""
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def download_feed_data(
    feed_type: str,
    feed_base64: str,
    start_date: str,
    end_date: str,
) -> tuple[int, int]:
    """Download parquet files for a date range.

    Returns:
        Tuple of (files_downloaded, files_skipped)
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    downloaded = 0
    skipped = 0

    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        # Build URL and local path matching the Hive partition structure
        url = f"{BASE_URL}/{feed_type}/date={date_str}/base64url={feed_base64}/data.parquet"

        local_dir = LOCAL_DATA_DIR / feed_type / f"date={date_str}" / f"base64url={feed_base64}"
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / "data.parquet"

        if local_file.exists():
            print(f"  [skip] {date_str} (already exists)")
            skipped += 1
        else:
            print(f"  [download] {date_str}...", end=" ", flush=True)
            try:
                urlretrieve(url, local_file)
                size_kb = local_file.stat().st_size / 1024
                print(f"OK ({size_kb:.1f} KB)")
                downloaded += 1
            except HTTPError as e:
                print(f"FAILED ({e.code}: {e.reason})")
                # Remove empty file if created
                if local_file.exists():
                    local_file.unlink()
            except Exception as e:
                print(f"FAILED ({e})")
                if local_file.exists():
                    local_file.unlink()

        current += timedelta(days=1)

    return downloaded, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Pre-download GTFS-RT parquet data for offline use",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using base64-encoded feed URL
  %(prog)s --feed-type vehicle_positions \\
           --feed-base64 aHR0cHM6Ly93d3czLnNlcHRhLm9yZy8... \\
           --start-date 2026-01-01 --end-date 2026-01-07

  # Using plain feed URL
  %(prog)s --feed-type vehicle_positions \\
           --feed-url "https://www3.septa.org/gtfsrt/septa-pa-us/Vehicle/rtVehiclePosition.pb" \\
           --start-date 2026-01-01 --end-date 2026-01-07
        """,
    )

    parser.add_argument(
        "--feed-type",
        required=True,
        choices=["vehicle_positions", "trip_updates", "service_alerts"],
        help="Type of feed to download",
    )

    # Either --feed-base64 or --feed-url is required
    feed_group = parser.add_mutually_exclusive_group(required=True)
    feed_group.add_argument(
        "--feed-base64",
        help="Base64url-encoded feed URL (from available_feeds.csv)",
    )
    feed_group.add_argument(
        "--feed-url",
        help="Plain feed URL (will be base64url-encoded)",
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=LOCAL_DATA_DIR,
        help=f"Output directory (default: {LOCAL_DATA_DIR})",
    )

    args = parser.parse_args()

    # Resolve feed_base64
    if args.feed_url:
        feed_base64 = encode_base64url(args.feed_url)
        print(f"Encoded feed URL: {feed_base64}")
    else:
        feed_base64 = args.feed_base64

    global LOCAL_DATA_DIR
    LOCAL_DATA_DIR = args.output_dir

    print(f"\nDownloading {args.feed_type} data:")
    print(f"  Feed: {feed_base64[:40]}...")
    print(f"  Date range: {args.start_date} to {args.end_date}")
    print(f"  Output: {LOCAL_DATA_DIR}/\n")

    downloaded, skipped = download_feed_data(
        args.feed_type,
        feed_base64,
        args.start_date,
        args.end_date,
    )

    print(f"\nDone! Downloaded: {downloaded}, Skipped: {skipped}")

    if downloaded > 0:
        print(f"\nFiles saved to: {LOCAL_DATA_DIR}/{args.feed_type}/")
        print("\nTo use local data in dbt, modify your macro to read from local files:")
        print(f"  read_parquet('data/{args.feed_type}/date=*/base64url={feed_base64}/data.parquet')")


if __name__ == "__main__":
    main()
