SELECT 
  *,
  uuid() as _uuid
FROM {{ read_gtfs_parquet('trip_updates') }}
WHERE date >= '{{ var("start_date") }}' AND date <= '{{ var("end_date") }}'
{% if is_incremental()%}
AND 1=0
{% endif %}