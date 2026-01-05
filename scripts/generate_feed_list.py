#!/usr/bin/env python3
"""
Discover available feeds from the GTFS-RT parquet bucket and generate
a CSV seed file for dbt.

Usage:
    uv run python scripts/generate_feed_list.py

This script queries the public GCS bucket to find all available feeds
and decodes their base64url-encoded URLs.
"""

import base64
import csv
import re
from pathlib import Path

from google.cloud import storage


BUCKET_NAME = "parquet.gtfsrt.io"
FEED_TYPES = ["vehicle_positions", "trip_updates", "service_alerts"]
OUTPUT_PATH = Path("seeds/available_feeds.csv")

# Prefix for HTTP-only feeds (no prefix = HTTPS)
HTTP_FEED_PREFIX = "~"


def decode_base64url(encoded: str) -> str:
    """Decode base64url string (add padding back for decoding)."""
    padded = encoded + "=" * (4 - len(encoded) % 4) if len(encoded) % 4 else encoded
    return base64.urlsafe_b64decode(padded).decode("utf-8")


def partition_key_to_url(key: str) -> str:
    """Convert partition key back to URL.

    The base64url encodes the full URL including scheme.
    The ~ prefix indicates HTTP feeds (for filtering), but
    the encoded portion still contains the full URL.
    """
    if key.startswith(HTTP_FEED_PREFIX):
        # HTTP feed - strip prefix and decode
        encoded = key[len(HTTP_FEED_PREFIX):]
        return decode_base64url(encoded)
    else:
        # HTTPS feed - decode directly
        return decode_base64url(key)


def discover_feeds() -> list[dict]:
    """Discover all feeds from the GCS bucket."""
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(BUCKET_NAME)

    feeds = []
    seen = set()

    for feed_type in FEED_TYPES:
        print(f"Scanning {feed_type}...")

        # List blobs with the feed type prefix
        blobs = bucket.list_blobs(prefix=f"{feed_type}/", delimiter="/")

        # Process prefixes (date partitions)
        for page in blobs.pages:
            for prefix in page.prefixes:
                # prefix looks like: vehicle_positions/date=2026-01-01/
                # We need to list inside to find base64url partitions
                date_blobs = bucket.list_blobs(prefix=prefix, delimiter="/")
                for date_page in date_blobs.pages:
                    for base64_prefix in date_page.prefixes:
                        # base64_prefix looks like:
                        # vehicle_positions/date=2026-01-01/base64url=ABC123/
                        match = re.search(r"base64url=([^/]+)", base64_prefix)
                        if match:
                            base64url = match.group(1)
                            key = (feed_type, base64url)
                            if key not in seen:
                                seen.add(key)
                                try:
                                    feed_url = partition_key_to_url(base64url)
                                    feeds.append({
                                        "feed_type": feed_type,
                                        "feed_url": feed_url,
                                        "base64url": base64url,
                                    })
                                    print(f"  Found: {feed_url[:60]}...")
                                except Exception as e:
                                    print(f"  Error decoding {base64url}: {e}")
                    # Only need to scan one date to find all feeds
                    break
                break

    return sorted(feeds, key=lambda x: (x["feed_type"], x["feed_url"]))


def main():
    print("Discovering feeds from gs://parquet.gtfsrt.io/...")
    feeds = discover_feeds()

    print(f"\nFound {len(feeds)} feeds. Writing to {OUTPUT_PATH}...")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["feed_type", "feed_url", "base64url"])
        writer.writeheader()
        writer.writerows(feeds)

    print("Done!")


if __name__ == "__main__":
    main()
