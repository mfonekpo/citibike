{{ config(materialized='view') }}


select
    source_id,
    station_id,

    feed_updated_at,
    last_reported_at,
    loaded_at,

    num_bikes_available,
    num_ebikes_available,
    num_bikes_disabled,

    num_docks_available,
    num_docks_disabled,

    is_installed,
    is_renting,
    is_returning,

    num_bikes_available = 0 as has_zero_available_bikes,
    num_docks_available = 0 as has_zero_available_docks

from {{ ref('stg_citibike__stream_station_status') }}

