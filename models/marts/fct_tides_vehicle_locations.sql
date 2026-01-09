with fct_tides_vehicle_locations as (
    select 
        -- tides schema columns
        location_ping_id,
        service_date,
        event_timestamp,
        trip_id_performed,
        trip_id_scheduled,
        trip_stop_sequence,
        scheduled_stop_sequence,
        vehicle_id,
        device_id,
        pattern_id,
        stop_id,
        current_status,
        latitude,
        longitude,
        gps_quality,
        heading,
        speed,
        odometer,
        schedule_deviation,
        headway_deviation,
        trip_type,
        schedule_relationship
    from {{ ref('fct_tides_vehicle_locations_quality') }}
    where is_valid
)

select * from fct_tides_vehicle_locations