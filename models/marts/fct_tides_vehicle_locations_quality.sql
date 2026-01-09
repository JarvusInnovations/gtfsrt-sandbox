with int_vehicle_locations as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['event_timestamp', 'vehicle_id']) }} as row_id
    from {{ ref('int_tides_vehicle_locations') }}
),

number_dups as (
    select 
        row_id,
        location_ping_id,
        -- TODO: I saw at least one case where same vehicle ID had two messages at same time with different trip IDs
        -- that would be good for additional cleaning
        row_number() over(partition by row_id order by location_ping_id) as _row_num
    from int_vehicle_locations
),

identify_dups as (
    select 
        row_id,
        count(*) > 1 as has_dups
    from number_dups
    group by row_id
),

fct_tides_vehicle_locations_quality as (
    select 
        -- tides schema columns
        int_vehicle_locations.location_ping_id,
        int_vehicle_locations.service_date,
        int_vehicle_locations.event_timestamp,
        int_vehicle_locations.trip_id_performed,
        int_vehicle_locations.trip_id_scheduled,
        int_vehicle_locations.trip_stop_sequence,
        int_vehicle_locations.scheduled_stop_sequence,
        int_vehicle_locations.vehicle_id,
        int_vehicle_locations.device_id,
        int_vehicle_locations.pattern_id,
        int_vehicle_locations.stop_id,
        int_vehicle_locations.current_status,
        int_vehicle_locations.latitude,
        int_vehicle_locations.longitude,
        int_vehicle_locations.gps_quality,
        int_vehicle_locations.heading,
        int_vehicle_locations.speed,
        int_vehicle_locations.odometer,
        int_vehicle_locations.schedule_deviation,
        int_vehicle_locations.headway_deviation,
        int_vehicle_locations.trip_type,
        int_vehicle_locations.schedule_relationship,

        -- quality columns
        -- as additional checks are implemented, is_valid should use "and" logic for all the checks 
        _row_num,
        _row_num = 1 as is_valid,
        _row_num = 1 as first_instance,
        identify_dups.has_dups
    from int_vehicle_locations
    left join number_dups 
        on int_vehicle_locations.row_id = number_dups.row_id
        and int_vehicle_locations.location_ping_id = number_dups.location_ping_id
    left join identify_dups
        on number_dups.row_id = identify_dups.row_id
    order by number_dups.row_id
)

select * from fct_tides_vehicle_locations_quality