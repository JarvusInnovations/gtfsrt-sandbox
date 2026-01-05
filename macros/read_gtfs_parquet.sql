{% macro read_gtfs_parquet(feed_type) %}
{#
    Generates a DuckDB read_parquet() call for GTFS-RT data from the public bucket.

    Args:
        feed_type: One of 'vehicle_positions', 'trip_updates', 'service_alerts'

    Uses dbt variables:
        - feed_base64: base64url-encoded feed URL
        - start_date: Start date (YYYY-MM-DD)
        - end_date: End date (YYYY-MM-DD)

    Example usage:
        SELECT * FROM {{ read_gtfs_parquet('vehicle_positions') }}
#}
{% set base_url = 'http://parquet.gtfsrt.io/' ~ feed_type %}
{% set feed_base64 = var('feed_base64') %}
{% set start_date = var('start_date') %}
{% set end_date = var('end_date') %}

read_parquet(
    list_transform(
        generate_series(
            DATE '{{ start_date }}',
            DATE '{{ end_date }}',
            INTERVAL 1 DAY
        ),
        d -> '{{ base_url }}/date=' || strftime(d, '%Y-%m-%d') || '/base64url={{ feed_base64 }}/data.parquet'
    ),
    union_by_name := true,
    filename := true
)
{% endmacro %}
