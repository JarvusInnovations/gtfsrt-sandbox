#!/usr/bin/env python3
"""
Download GTFS-RT parquet data for use with dbt models.

This script downloads parquet files from the public data source to a local
directory, allowing dbt models to query local files.

Usage:
    # Download default sample data (AC Transit, all feed types)
    uv run python scripts/download_data.py --defaults

    # Download defaults for a specific date
    uv run python scripts/download_data.py --defaults --date 2026-01-20

    # Download a specific feed type and date range
    uv run python scripts/download_data.py \
        --feed-type vehicle_positions \
        --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \
        --start-date 2026-01-01 \
        --end-date 2026-01-07

See docs/downloading_data.md for more examples.
"""

import argparse
import base64
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import HTTPError


BASE_URL = "http://parquet.gtfsrt.io"
LOCAL_DATA_DIR = Path("data")

# Default feeds: AC Transit (smaller, reliable, 106 routes)
DEFAULT_FEEDS = {
    "vehicle_positions": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC92ZWhpY2xlcw",
    "trip_updates": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC90cmlwdXBkYXRlcw",
    "service_alerts": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC9hbGVydHM",
}
DEFAULT_DATE = "2026-01-24"


def encode_base64url(url: str) -> str:
    """Encode URL to base64url (without padding)."""
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def download_feed_data(
    feed_type: str,
    feed_base64: str,
    start_date: str,
    end_date: str,
    output_dir: Path,
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

        local_dir = output_dir / feed_type / f"date={date_str}" / f"base64url={feed_base64}"
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


def download_defaults(date: str, output_dir: Path) -> dict[str, tuple[int, int]]:
    """Download all default feeds for a single date.

    Returns:
        Dict mapping feed_type to (downloaded, skipped) counts
    """
    results = {}

    for feed_type, feed_base64 in DEFAULT_FEEDS.items():
        print(f"\n{feed_type}:")
        downloaded, skipped = download_feed_data(
            feed_type=feed_type,
            feed_base64=feed_base64,
            start_date=date,
            end_date=date,
            output_dir=output_dir,
        )
        results[feed_type] = (downloaded, skipped)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Download GTFS-RT parquet data for dbt models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download sample data (AC Transit, all feed types)
  %(prog)s --defaults

  # Download defaults for a specific date
  %(prog)s --defaults --date 2026-01-20

  # Download a specific feed
  %(prog)s --feed-type vehicle_positions \\
           --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \\
           --start-date 2026-01-01 --end-date 2026-01-07

See docs/downloading_data.md for more examples.
        """,
    )

    # Default mode
    parser.add_argument(
        "--defaults",
        action="store_true",
        help="Download AC Transit data for all feed types (vehicle_positions, trip_updates, service_alerts)",
    )
    parser.add_argument(
        "--date",
        default=DEFAULT_DATE,
        help=f"Date for --defaults mode (default: {DEFAULT_DATE})",
    )

    # Specific feed mode
    parser.add_argument(
        "--feed-type",
        choices=["vehicle_positions", "trip_updates", "service_alerts"],
        help="Type of feed to download",
    )

    # Either --feed-base64 or --feed-url
    feed_group = parser.add_mutually_exclusive_group()
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
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=LOCAL_DATA_DIR,
        help=f"Output directory (default: {LOCAL_DATA_DIR})",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.defaults:
        # Default mode: download all feeds for a single date
        print(f"Downloading AC Transit sample data for {args.date}")
        print(f"Output: {args.output_dir}/")

        results = download_defaults(args.date, args.output_dir)

        print("\n" + "=" * 50)
        print("Summary:")
        total_downloaded = 0
        total_skipped = 0
        for feed_type, (downloaded, skipped) in results.items():
            status = "✓" if downloaded > 0 or skipped > 0 else "✗"
            print(f"  {status} {feed_type}: {downloaded} downloaded, {skipped} skipped")
            total_downloaded += downloaded
            total_skipped += skipped

        print(f"\nTotal: {total_downloaded} files downloaded, {total_skipped} skipped")

        if total_downloaded > 0 or total_skipped > 0:
            print(f"\nData saved to: {args.output_dir}/")
            print("\nNext steps:")
            print("  uv run dbt run              # Transform the data")
            print("  duckdb workshop.duckdb -ui  # Query the results")

    else:
        # Specific feed mode: validate required arguments
        if not args.feed_type:
            parser.error("--feed-type is required when not using --defaults")
        if not args.feed_base64 and not args.feed_url:
            parser.error("--feed-base64 or --feed-url is required when not using --defaults")
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required when not using --defaults")

        # Resolve feed_base64
        if args.feed_url:
            feed_base64 = encode_base64url(args.feed_url)
            print(f"Encoded feed URL: {feed_base64}")
        else:
            feed_base64 = args.feed_base64

        print(f"\nDownloading {args.feed_type} data:")
        print(f"  Feed: {feed_base64[:40]}...")
        print(f"  Date range: {args.start_date} to {args.end_date}")
        print(f"  Output: {args.output_dir}/\n")

        downloaded, skipped = download_feed_data(
            args.feed_type,
            feed_base64,
            args.start_date,
            args.end_date,
            args.output_dir,
        )

        print(f"\nDone! Downloaded: {downloaded}, Skipped: {skipped}")

        if downloaded > 0:
            print(f"\nFiles saved to: {args.output_dir}/{args.feed_type}/")


if __name__ == "__main__":
    main()
