with trips as (
    SELECT
        ride_id,
        rideable_type,
        member_casual as membership,
        started_at,
        ended_at,

        start_station_id as start_station_code,
        end_station_id as end_station_code,

        start_station_name,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        source_locator,
        ingested_at
    FROM {{ ref("stg_citibike__raw_citibike_tripdata") }}
    where ended_at > started_at
),


stations as (

    select
        nullIf(station_id, '') as station_id,
        station_code

    from {{ ref('dim_stations') }}

),


enriched as (

    select
        t.*,

        departure.station_id as start_station_id,
        arrival.station_id as end_station_id,

        dateDiff(
            'millisecond',
            t.started_at,
            t.ended_at
        ) / 1000.0 as duration_seconds

    from trips as t

    left join stations as departure
        on t.start_station_code = departure.station_code

    left join stations as arrival
        on t.end_station_code = arrival.station_code

)


select
    enriched.*,

    isNotNull(start_station_id) as is_start_station_matched,
    isNotNull(end_station_id) as is_end_station_matched,

    (
        duration_seconds < 60
        or duration_seconds > 86400
    ) as is_duration_outlier

from enriched


