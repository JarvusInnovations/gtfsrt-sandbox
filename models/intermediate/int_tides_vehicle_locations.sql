-- with reference to https://github.com/evansiroky/gtfs-rt-to-tides/blob/main/parse_vehicle_positions_for_day.py

with stg as (
    select * from {{ ref('stg_vehicle_positions') }}
),



int_tides_vehicle_locations as (
    select
        -- this is not guaranteed to be unique
        {{ dbt_utils.generate_surrogate_key(['feed_timestamp', 'vehicle_timestamp', 'vehicle_id']) }} as location_ping_id,
        -- TODO: we would need GTFS schedule or a manual specification to do a feed time zone lookup here
        -- for now, just use Eastern Time and hope for the best
        timezone(
            -- add four hours to handle service day offset
            'America/New_York', vehicle_timestamp + interval 4 hour
        )::date as service_date,
        vehicle_timestamp as event_timestamp,
        trip_id as trip_id_performed,
        -- TODO: requires GTFS schedule lookup
        null as trip_id_scheduled,
        current_stop_sequence as trip_stop_sequence,
        -- TODO: requires GTFS schedule lookup
        null as scheduled_stop_sequence,
        vehicle_id as vehicle_id,
        -- TODO: TIDES schema change proposed: https://github.com/TIDES-transit/TIDES/pull/251
        null as device_id,
        null as pattern_id,
        stop_id,
        current_status,
        latitude,
        longitude,
        -- probably out of scope for this project
        -- would require access to supplemental data or outside data science work to assess
        null as gps_quality,
        bearing as heading,
        speed,
        odometer,
        -- TODO: could calculate from GTFS schedule
        null as schedule_deviation,
        -- TODO: could calculate from GTFS schedule
        null as headway_deviation,
        -- could default to in service assuming that most data in GTFSRT is in service (since customer facing) but that's an assumption
        null as trip_type,
        -- in TIDES this is stop rather than trip-level: https://github.com/TIDES-transit/TIDES/issues/252
        null as schedule_relationship
    from stg
)

select * from int_tides_vehicle_locations