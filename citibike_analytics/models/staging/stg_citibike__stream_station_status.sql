with source as (

    select
        source_id,
        snapshot_id,
        loaded_at,
        raw_payload
    from {{ source('citibike_raw', 'stream_stations_status_raw') }}

),


identified_snapshots as (

    select
        source_id,
        snapshot_id,

        min(loaded_at) as first_loaded_at,
        any(raw_payload) as snapshot_payload,
        uniqExact(raw_payload) as payload_versions

    from source

    where snapshot_id is not null

    group by
        source_id,
        snapshot_id

    having payload_versions = 1

),

accepted_responses as (

    select
        source_id,
        snapshot_id,
        first_loaded_at as loaded_at,
        snapshot_payload as raw_payload

    from identified_snapshots

    union all

    select
        source_id,
        snapshot_id,
        loaded_at,
        raw_payload

    from source

    where snapshot_id is null

),


unpacked as (

    select
        source_id,
        snapshot_id,
        loaded_at,

        toDateTime(
            JSONExtract(raw_payload, 'last_updated', 'Nullable(Int64)'),
            'UTC'
        ) as feed_updated_at,

        station_json

    from accepted_responses
    array join
        JSONExtractArrayRaw(
            raw_payload, 'data', 'stations'
        ) as station_json

),


cleaned as (

    select
        source_id,
        snapshot_id,
        loaded_at,
        feed_updated_at,


        nullIf(
            JSONExtractString(station_json, 'station_id'),
            ''
        ) as station_id,


        JSONExtract(
            station_json, 'num_bikes_available', 'Nullable(UInt32)'
        ) as num_bikes_available,
    

        JSONExtract(
            station_json, 'num_bikes_disabled', 'Nullable(UInt32)'
        ) as num_bikes_disabled,


        JSONExtract(
            station_json, 'num_docks_available', 'Nullable(UInt32)'
        ) as num_docks_available,


        JSONExtract(
            station_json, 'num_docks_disabled', 'Nullable(UInt32)'
        ) as num_docks_disabled,


        JSONExtract(
            station_json, 'num_ebikes_available', 'Nullable(UInt32)'
        ) as num_ebikes_available,


        JSONExtract(
            station_json, 'is_installed', 'Nullable(UInt32)'
        ) as is_installed,


        JSONExtract(
            station_json, 'is_renting', 'Nullable(UInt32)'
        ) as is_renting,


        JSONExtract(
            station_json, 'is_returning', 'Nullable(UInt32)'
        ) as is_returning,


        toDateTime(
            JSONExtract(station_json, 'last_reported', 'Nullable(Int64)'),
            'UTC'
        ) as last_reported_at

    from unpacked

)

select * from cleaned