with source as (

    select
        source_id,
        loaded_at,
        raw_payload
    from {{ source('citibike_raw', 'stream_system_regions_raw') }}
),


unpacked as (
    select
        source_id,
        loaded_at,
        toDateTime(
            JSONExtract(raw_payload, 'last_updated', 'Nullable(Int64)'),
            'UTC'
        ) as feed_updated_at,
        region_json

    from source
    array JOIN
        JSONExtractArrayRaw(
            raw_payload, 'data', 'regions'
        ) as region_json
),


cleaned as (
    SELECT
        source_id,
        loaded_at,
        feed_updated_at,
        
        nullif(
            JSONExtract(region_json, 'region_id', 'Nullable(String)'),
            ''
        ) as region_id,

        nullif(
            JSONExtract(region_json, 'name', 'Nullable(String)'),
            ''
        ) as region_name

    from unpacked
)


select * from cleaned