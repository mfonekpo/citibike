with source as (
    select * from {{ source("citibike_raw", "raw_citibike_tripdata") }}
),
cleaned as (
    select
        nullif(trimBoth(ride_id), '') as ride_id,
        nullif(trimBoth(rideable_type), '') as rideable_type,
        cast(started_at as Nullable(DateTime64(3, 'UTC'))) as started_at,
        cast(ended_at as Nullable(DateTime64(3, 'UTC'))) as ended_at,
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
        cast(ingested_at as DateTime64(3, 'UTC')) as ingested_at
    from source
)

select * from cleaned
