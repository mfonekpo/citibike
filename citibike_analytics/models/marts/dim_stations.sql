with stations as (
    select
        station_id,
        short_name as station_code,
        name as station_name,
        region_id,
        lat as latitude,
        lon as longitude,
        capacity,
        feed_updated_at,
        loaded_at
    from {{ ref('stg_citibike__stream_station_information') }}
    where station_id is not NULL
),


ranked as (
    select
        stations.*,

        row_number() over (
            partition by station_id
            order by
                feed_updated_at desc nulls last,
                loaded_at desc
        ) as row_num
    from stations
),


latest as (
    select *
    from ranked
    where row_num = 1
)

select
    s.station_id,
    s.station_code,
    s.station_name,
    s.region_id,
    r.region_name,
    s.latitude,
    s.longitude,
    s.capacity,
    s.feed_updated_at,
    s.loaded_at

from latest as s

left join {{ ref('dim_regions') }} as r
    on s.region_id = r.region_id