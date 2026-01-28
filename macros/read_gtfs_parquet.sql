{% macro read_gtfs_parquet(feed_type) %}
{#
    Reads GTFS-RT parquet data from the local data/ directory.
    Downloads all available dates and feeds for the given feed type.

    Args:
        feed_type: One of 'vehicle_positions', 'trip_updates', 'service_alerts'

    Returns:
        A read_parquet() call that reads all downloaded data for this feed type.
        The 'date' and 'base64url' columns are available from Hive partitioning.

    Example usage:
        SELECT * FROM {{ read_gtfs_parquet('vehicle_positions') }}

    Before running dbt, download data with:
        uv run python scripts/download_data.py --defaults
#}
read_parquet(
    'data/{{ feed_type }}/date=*/base64url=*/data.parquet',
    hive_partitioning = true
)
{% endmacro %}
