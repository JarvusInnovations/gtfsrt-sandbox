{% macro read_gtfs_parquet(feed_type, feed_base64=none) %}
{#
    Generates a DuckDB read_parquet() call for GTFS-RT data from the public GCS bucket.
    Uses gs:// URLs with glob patterns for efficient directory listing.

    Args:
        feed_type: One of 'vehicle_positions', 'trip_updates', 'service_alerts'
        feed_base64: Optional base64url-encoded feed URL. If not provided,
                     uses the corresponding dbt variable (e.g., vehicle_positions_feed)

    Uses dbt variables:
        - {feed_type}_feed: base64url-encoded feed URL (if feed_base64 not provided)
        - start_date: Start date for filtering (YYYY-MM-DD)
        - end_date: End date for filtering (YYYY-MM-DD)

    Returns:
        A read_parquet() call with hive_partitioning enabled.
        The 'date' column is available for filtering in your query.

    Example usage:
        SELECT * FROM {{ read_gtfs_parquet('vehicle_positions') }}
        WHERE date >= '{{ var("start_date") }}' AND date <= '{{ var("end_date") }}'
#}
{% set feed_base64_value = feed_base64 if feed_base64 else var(feed_type ~ '_feed') %}

read_parquet(
    'gs://parquet.gtfsrt.io/{{ feed_type }}/date=*/base64url={{ feed_base64_value }}/data.parquet',
    hive_partitioning = true,
    filename = true
)
{% endmacro %}
