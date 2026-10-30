with source as (
    
    select
        source_id,
        loaded_at,
        raw_payload
    from {{ source('citibike_raw', 'stream_station_information_raw') }}
),



unpacked as (
    SELECT
        source_id,
        loaded_at,

        toDateTime(
            JSONExtract(raw_payload, 'last_updated', 'Nullable(Int64)'),
            'UTC'
        ) as feed_updated_at,

        station_json
    
    from source
    array JOIN
        JSONExtractArrayRaw(
            raw_payload, 'data', 'stations'
        ) as station_json
),



cleaned as (

    SELECT
        source_id,
        loaded_at,
        feed_updated_at,


        nullIf(
            JSONExtractString(station_json, 'station_id'),
            ''
        ) as station_id,


        nullif(
            JSONExtractString(station_json, 'name'),
            ''
        ) as name,


        nullif(
            JSONExtractString(station_json, 'short_name'),
            ''
        ) as short_name,


        JSONExtract(station_json, 'lon', 'Nullable(Float64)') as lon,


        JSONExtract(station_json, 'lat', 'Nullable(Float64)') as lat,


        nullif(
            JSONExtractString(station_json, 'region_id'),
            ''
        ) as region_id,


        JSONExtract(
            station_json, 'capacity', 'Nullable(UInt32)'
        ) as capacity

    from unpacked
)


select * FROM cleaned