{% docs tides_location_ping_id %}
Identifies the recorded vehicle location event.

**TIDES Specification:**
- Primary key field
- Required, must be unique
- Type: string
{% enddocs %}

{% docs tides_service_date %}
Service date for the vehicle location event.

**TIDES Specification:**
- Type: date
- References GTFS service calendars
{% enddocs %}

{% docs tides_event_timestamp %}
Recorded event timestamp for the vehicle location.

**TIDES Specification:**
- Required field
- Type: datetime
{% enddocs %}

{% docs tides_trip_id_performed %}
Identifies the trip performed by the vehicle.

**TIDES Specification:**
- Type: string
- References `trips_performed.trip_id_performed`
- If `trips_performed` table is used, this must be populated to link to it
{% enddocs %}

{% docs tides_trip_id_scheduled %}
Identifies the scheduled trip.

**TIDES Specification:**
- Type: string
- References `trips_performed.trip_id_scheduled`
{% enddocs %}

{% docs tides_trip_stop_sequence %}
The actual order of stops visited within a performed trip.

**TIDES Specification:**
- Type: positive integer
- References `stop_visits.trip_stop_sequence`
- Values must start at 1 and be consecutive along the trip

**Example:** A bus departs the first stop and detours around the second and third scheduled stops, visiting one unscheduled stop and resuming regular service at the 4th scheduled stop. The `scheduled_stop_sequence` is [1, null, 4], and the `trip_stop_sequence` is [1, 2, 3].
{% enddocs %}

{% docs tides_scheduled_stop_sequence %}
Scheduled order of stops for a particular trip.

**TIDES Specification:**
- Type: positive integer (minimum: 0)
- References GTFS `stop_times.stop_sequence`
- Values must increase along the trip but do not need to be consecutive
{% enddocs %}

{% docs tides_vehicle_id %}
Identifies the transit vehicle.

**TIDES Specification:**
- Required field
- Type: string
- References `vehicles.vehicle_id`
{% enddocs %}

{% docs tides_device_id %}
Identifies the device that recorded the vehicle location.

**TIDES Specification:**
- Type: string
- References `devices.device_id`
- May be null if only a single device is reporting vehicle location and the `device_id` is not distinct from `vehicle_id`
{% enddocs %}

{% docs tides_pattern_id %}
Identifies the unique stop-path for a trip.

**TIDES Specification:**
- Type: string
- May be distinct from GTFS `shapes.shape_id`
{% enddocs %}

{% docs tides_stop_id %}
Identifies the stop the vehicle is approaching or currently serving.

**TIDES Specification:**
- Type: string
- References GTFS `stops.stop_id`
{% enddocs %}

{% docs tides_current_status %}
Indicates the status of the vehicle in reference to a stop.

**TIDES Specification:**
- Type: string (enum)
- References GTFS-Realtime
- Allowed values:
  - `Incoming at` - Vehicle is approaching the stop
  - `Stopped at` - Vehicle is stopped at the stop
  - `In transit to` - Vehicle is in transit to the stop
{% enddocs %}

{% docs tides_latitude %}
Vehicle position latitude coordinate.

**TIDES Specification:**
- Type: number
- Degrees North, in the WGS-84 coordinate system
- Range: -90 to 90
- References GTFS-Realtime
{% enddocs %}

{% docs tides_longitude %}
Vehicle position longitude coordinate.

**TIDES Specification:**
- Type: number
- Degrees East, in the WGS-84 coordinate system
- Range: -180 to 180
- References GTFS-Realtime
{% enddocs %}

{% docs tides_gps_quality %}
Indicates the quality of data and communication provided by the GPS.

**TIDES Specification:**
- Type: string (enum)
- Allowed values:
  - `Excellent`
  - `Good`
  - `Poor`
{% enddocs %}

{% docs tides_heading %}
Heading or bearing of the vehicle.

**TIDES Specification:**
- Type: number
- Degrees, clockwise from true north (0° = north, 90° = east)
- Range: 0 to 360
- Can be compass bearing or direction towards the next stop
- Should not be deduced from sequence of previous positions
- References GTFS-Realtime
{% enddocs %}

{% docs tides_speed %}
Momentary speed measured by the vehicle.

**TIDES Specification:**
- Type: number
- Units: meters per second
- Minimum: 0
- References GTFS-Realtime
{% enddocs %}

{% docs tides_odometer %}
Odometer value for the vehicle.

**TIDES Specification:**
- Type: number
- Units: meters
- Minimum: 0
- References GTFS-Realtime
{% enddocs %}

{% docs tides_schedule_deviation %}
Indicates schedule adherence in seconds.

**TIDES Specification:**
- Type: integer
- Negative value = vehicle is early
- Positive value = vehicle is late
- Note: An unscheduled trip would not have a schedule deviation
{% enddocs %}

{% docs tides_headway_deviation %}
Indicates headway adherence in seconds.

**TIDES Specification:**
- Type: integer
- Negative value = shorter than scheduled headway
- Positive value = longer than scheduled headway
{% enddocs %}

{% docs tides_trip_type %}
Indicates status of travel with regard to service.

**TIDES Specification:**
- Type: string (enum)
- Allowed values:
  - `In service` - Regular passenger service
  - `Deadhead` - Moving without passengers
  - `Layover` - Resting between trips
  - `Pullout` - Leaving garage to start service
  - `Pullin` - Returning to garage after service
  - `Extra Pullout`
  - `Extra Pullin`
  - `Deadhead To Layover`
  - `Deadhead From Layover`
  - `Other not in service`
{% enddocs %}

{% docs tides_schedule_relationship %}
Indicates the status of the stop in relation to the schedule.

**TIDES Specification:**
- Type: string (enum)
- Allowed values:
  - `Scheduled` - Stop is served as planned
  - `Skipped` - Stop was skipped
  - `Added` - Unscheduled stop was added
  - `Missing` - Expected stop data is missing
{% enddocs %}
