with source as (
    select * from {{ source("citibike_raw", "raw_citibike_tripdata") }}
),
cleaned as (
    select
        nullif(trimBoth(ride_id), '') as ride_id,
        nullif(trimBoth(rideable_type), '') as rideable_type,

        toTimeZone(
            toDateTime64OrNull(
                trimBoth(started_at), 3, 'America/New_York'
            ),
            'UTC'
        ) as started_at,

        toTimeZone(
            toDateTime64OrNull(
                trimBoth(ended_at), 3, 'America/New_York'
            ),
            'UTC'
        ) as ended_at,

        nullif(trimBoth(start_station_name), '') as start_station_name,
        nullif(trimBoth(start_station_id), '') as start_station_id,
        nullif(trimBoth(end_station_name), '') as end_station_name,
        nullif(trimBoth(end_station_id), '') as end_station_id,
        toFloat64OrNull(start_lat) as start_lat,
        toFloat64OrNull(start_lng) as start_lng,
        toFloat64OrNull(end_lat) as end_lat,
        toFloat64OrNull(end_lng) as end_lng,
        nullif(trimBoth(member_casual), '') as member_casual,
        nullif(trimBoth(source_locator), '') as source_locator,
        batch_id,
        release_month,
        cast(ingested_at as DateTime64(3, 'UTC')) as ingested_at,

        -- Preserve original source values for validation and quarantine.

        src.started_at AS raw_started_at,
        src.ended_at AS raw_ended_at,

        src.start_lat AS raw_start_lat,
        src.start_lng AS raw_start_lng,

        src.end_lat AS raw_end_lat,
        src.end_lng AS raw_end_lng
    from source as src
)

select * from cleaned
