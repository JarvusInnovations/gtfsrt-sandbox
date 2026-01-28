#!/usr/bin/env python3
"""
Download GTFS-RT parquet data for use with dbt models.

This script downloads parquet files from the public data source to a local
directory, allowing dbt models to query local files.

Usage:
    # List available agencies
    uv run python scripts/download_data.py --list

    # Download default sample data (AC Transit, all feed types)
    uv run python scripts/download_data.py --defaults

    # Download all feeds for a specific agency
    uv run python scripts/download_data.py --agency septa --date 2026-01-20

    # Download a specific feed type and date range (advanced)
    uv run python scripts/download_data.py \
        --feed-type vehicle_positions \
        --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \
        --start-date 2026-01-01 \
        --end-date 2026-01-07

See docs/downloading_data.md for more examples.
"""

import argparse
import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError


BASE_URL = "http://parquet.gtfsrt.io"
INVENTORY_URL = "http://storage.googleapis.com/parquet.gtfsrt.io/inventory.json"
LOCAL_DATA_DIR = Path("data")
DEFAULT_AGENCY = "actransit"
DEFAULT_DATE = "2026-01-24"


def encode_base64url(url: str) -> str:
    """Encode URL to base64url (without padding)."""
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")


def fetch_inventory() -> list[dict]:
    """Fetch feed inventory from remote source."""
    try:
        with urlopen(INVENTORY_URL, timeout=10) as response:
            return json.load(response)
    except Exception as e:
        print(f"Warning: Could not fetch inventory ({e})")
        return []


def get_agencies(inventory: list[dict]) -> dict:
    """Group inventory by agency_id."""
    agencies = {}
    for feed in inventory:
        aid = feed["agency_id"]
        if aid not in agencies:
            agencies[aid] = {
                "name": feed["agency_name"],
                "feeds": {},
                "date_min": feed["date_min"],
                "date_max": feed["date_max"],
            }
        # Track feeds and update date range
        agencies[aid]["feeds"][feed["feed_type"]] = feed
        agencies[aid]["date_min"] = min(agencies[aid]["date_min"], feed["date_min"])
        agencies[aid]["date_max"] = max(agencies[aid]["date_max"], feed["date_max"])
    return agencies


def list_agencies(inventory: list[dict]) -> None:
    """Display available agencies."""
    agencies = get_agencies(inventory)

    print("\nAvailable agencies:\n")
    print(f"  {'Agency ID':<22} {'Agency Name':<32} {'Feeds':<6} {'Date Range'}")
    print(f"  {'-' * 22} {'-' * 32} {'-' * 6} {'-' * 24}")

    for aid in sorted(agencies.keys()):
        info = agencies[aid]
        feed_count = len(info["feeds"])
        print(f"  {aid:<22} {info['name']:<32} {feed_count:<6} {info['date_min']} to {info['date_max']}")

    print(f"\nUse --agency <id> to download all feeds for an agency.")
    print(f"Example: uv run python scripts/download_data.py --agency septa --date 2026-01-20")


def format_size(bytes_size: int) -> str:
    """Format bytes as human-readable size."""
    if bytes_size >= 1_000_000_000:
        return f"{bytes_size / 1_000_000_000:.1f} GB"
    elif bytes_size >= 1_000_000:
        return f"{bytes_size / 1_000_000:.1f} MB"
    elif bytes_size >= 1_000:
        return f"{bytes_size / 1_000:.1f} KB"
    return f"{bytes_size} B"


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
                size = local_file.stat().st_size
                print(f"OK ({format_size(size)})")
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


def download_agency(agency_id: str, date: str, output_dir: Path, inventory: list[dict]) -> dict[str, tuple[int, int]]:
    """Download all feeds for an agency.

    Returns:
        Dict mapping feed_type to (downloaded, skipped) counts
    """
    agencies = get_agencies(inventory)

    if agency_id not in agencies:
        available = ", ".join(sorted(agencies.keys()))
        print(f"Error: Unknown agency '{agency_id}'")
        print(f"Available agencies: {available}")
        return {}

    agency = agencies[agency_id]
    feeds = agency["feeds"]

    # Validate date is in range
    if date < agency["date_min"] or date > agency["date_max"]:
        print(f"Warning: Date {date} is outside available range ({agency['date_min']} to {agency['date_max']})")

    # Estimate sizes
    total_bytes = 0
    print(f"\nDownloading {agency['name']} data for {date}:")
    for feed_type in sorted(feeds.keys()):
        feed = feeds[feed_type]
        # Estimate per-day size (total_bytes / days in range)
        days_available = (datetime.strptime(feed["date_max"], "%Y-%m-%d") -
                         datetime.strptime(feed["date_min"], "%Y-%m-%d")).days + 1
        estimated_size = feed["total_bytes"] // max(days_available, 1)
        total_bytes += estimated_size
        print(f"  {feed_type}: ~{format_size(estimated_size)}")
    print(f"  Total: ~{format_size(total_bytes)}")

    results = {}
    for feed_type in sorted(feeds.keys()):
        feed = feeds[feed_type]
        print(f"\n{feed_type}:")
        downloaded, skipped = download_feed_data(
            feed_type=feed_type,
            feed_base64=feed["base64url"],
            start_date=date,
            end_date=date,
            output_dir=output_dir,
        )
        results[feed_type] = (downloaded, skipped)

    return results


def print_summary(results: dict[str, tuple[int, int]], output_dir: Path) -> None:
    """Print download summary."""
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
        print(f"\nData saved to: {output_dir}/")
        print("\nNext steps:")
        print("  uv run dbt run              # Transform the data")
        print("  duckdb workshop.duckdb -ui  # Query the results")


def main():
    parser = argparse.ArgumentParser(
        description="Download GTFS-RT parquet data for dbt models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available agencies
  %(prog)s --list

  # Download sample data (AC Transit, all feed types)
  %(prog)s --defaults

  # Download all feeds for a specific agency
  %(prog)s --agency septa --date 2026-01-20

  # Download a specific feed (advanced)
  %(prog)s --feed-type vehicle_positions \\
           --feed-url "https://api.actransit.org/transit/gtfsrt/vehicles" \\
           --start-date 2026-01-01 --end-date 2026-01-07

See docs/downloading_data.md for more examples.
        """,
    )

    # Discovery mode
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available agencies from inventory",
    )

    # Simple download modes
    parser.add_argument(
        "--defaults",
        action="store_true",
        help="Download AC Transit data for all feed types",
    )
    parser.add_argument(
        "--agency",
        help="Download all feeds for an agency (e.g., actransit, septa, vta)",
    )
    parser.add_argument(
        "--date",
        default=DEFAULT_DATE,
        help=f"Date for --defaults/--agency mode (default: {DEFAULT_DATE})",
    )

    # Advanced: specific feed mode
    parser.add_argument(
        "--feed-type",
        choices=["vehicle_positions", "trip_updates", "service_alerts"],
        help="Type of feed to download (advanced)",
    )
    feed_group = parser.add_mutually_exclusive_group()
    feed_group.add_argument(
        "--feed-base64",
        help="Base64url-encoded feed URL (advanced)",
    )
    feed_group.add_argument(
        "--feed-url",
        help="Plain feed URL (advanced)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date for date range (advanced)",
    )
    parser.add_argument(
        "--end-date",
        help="End date for date range (advanced)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=LOCAL_DATA_DIR,
        help=f"Output directory (default: {LOCAL_DATA_DIR})",
    )

    args = parser.parse_args()

    # Handle --list mode
    if args.list:
        inventory = fetch_inventory()
        if not inventory:
            print("Error: Could not fetch inventory. Check your internet connection.")
            return
        list_agencies(inventory)
        return

    # Handle --defaults mode
    if args.defaults:
        inventory = fetch_inventory()
        if not inventory:
            print("Warning: Could not fetch inventory, using hardcoded defaults.")
            # Fallback to hardcoded defaults
            from collections import namedtuple
            Feed = namedtuple("Feed", ["base64url"])
            results = {}
            print(f"Downloading AC Transit sample data for {args.date}")
            print(f"Output: {args.output_dir}/")
            default_feeds = {
                "vehicle_positions": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC92ZWhpY2xlcw",
                "trip_updates": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC90cmlwdXBkYXRlcw",
                "service_alerts": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnNydC9hbGVydHM",
            }
            for feed_type, feed_base64 in default_feeds.items():
                print(f"\n{feed_type}:")
                downloaded, skipped = download_feed_data(
                    feed_type=feed_type,
                    feed_base64=feed_base64,
                    start_date=args.date,
                    end_date=args.date,
                    output_dir=args.output_dir,
                )
                results[feed_type] = (downloaded, skipped)
            print_summary(results, args.output_dir)
            return

        results = download_agency(DEFAULT_AGENCY, args.date, args.output_dir, inventory)
        if results:
            print_summary(results, args.output_dir)
        return

    # Handle --agency mode
    if args.agency:
        inventory = fetch_inventory()
        if not inventory:
            print("Error: Could not fetch inventory. Check your internet connection.")
            return
        results = download_agency(args.agency, args.date, args.output_dir, inventory)
        if results:
            print_summary(results, args.output_dir)
        return

    # Handle advanced specific feed mode
    if args.feed_type:
        if not args.feed_base64 and not args.feed_url:
            parser.error("--feed-base64 or --feed-url is required with --feed-type")
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required with --feed-type")

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
        return

    # No mode specified
    parser.print_help()
    print("\n\nQuick start:")
    print("  %(prog)s --list      # See available agencies" % {"prog": parser.prog})
    print("  %(prog)s --defaults  # Download AC Transit sample data" % {"prog": parser.prog})


if __name__ == "__main__":
    main()
