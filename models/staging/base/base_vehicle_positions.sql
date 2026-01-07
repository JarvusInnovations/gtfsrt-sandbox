{{
  config(
    materialized='incremental'
  )
}}

SELECT * FROM {{ read_gtfs_parquet('vehicle_positions') }}
WHERE date >= '{{ var("start_date") }}' AND date <= '{{ var("end_date") }}'
{% if is_incremental()%}
AND 1=0
{% endif %}