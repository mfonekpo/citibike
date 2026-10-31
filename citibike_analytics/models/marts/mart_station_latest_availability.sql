{{ config(materialized='view') }}

with ranked as (

    select
        *,

        row_number() over (
            partition by station_id
            order by
                feed_updated_at desc nulls last,
                last_reported_at desc nulls last,
                loaded_at desc,
                snapshot_id desc nulls last
        ) as row_num

    from {{ ref('fct_station_availability') }}

    where station_id is not null

),

enriched as (

    select
        a.source_id,
        a.snapshot_id,
        a.station_id,

        d.station_code,
        d.station_name,
        d.region_id,
        d.region_name,
        d.latitude,
        d.longitude,
        d.capacity as station_capacity,

        a.feed_updated_at,
        a.last_reported_at,
        a.loaded_at,

        a.num_bikes_available,
        a.num_ebikes_available,
        a.num_bikes_disabled,
        a.num_docks_available,
        a.num_docks_disabled,

        a.is_installed,
        a.is_renting,
        a.is_returning,

        a.has_zero_available_bikes,
        a.has_zero_available_docks,

        isNotNull(
            nullIf(d.station_id, '')
        ) as has_station_metadata,

        now64(3, 'UTC') as evaluated_at

    from ranked as a

    left join {{ ref('dim_stations') }} as d
        on a.station_id = d.station_id

    where a.row_num = 1

),

freshness as (

    select
        enriched.*,

        ifNull(
            (
                feed_updated_at between
                    loaded_at - interval {{ var('max_report_age_seconds', 300) | int }} second
                    and loaded_at + interval {{ var('allowed_future_clock_skew_seconds', 60) | int }} second
            )
            and
            (
                last_reported_at between
                    loaded_at - interval {{ var('max_report_age_seconds', 300) | int }} second
                    and loaded_at + interval {{ var('allowed_future_clock_skew_seconds', 60) | int }} second
            ),
            0
        ) as was_fresh_at_load,

        ifNull(
            (
                feed_updated_at between
                    evaluated_at - interval {{ var('max_report_age_seconds', 300) | int }} second
                    and evaluated_at + interval {{ var('allowed_future_clock_skew_seconds', 60) | int }} second
            )
            and
            (
                last_reported_at between
                    evaluated_at - interval {{ var('max_report_age_seconds', 300) | int }} second
                    and evaluated_at + interval {{ var('allowed_future_clock_skew_seconds', 60) | int }} second
            ),
            0
        ) as is_fresh_now

    from enriched

)

select
    freshness.*,

    ifNull(
        was_fresh_at_load = 1
        and is_fresh_now = 1
        and is_installed = 1
        and is_renting = 1
        and isNotNull(num_bikes_available),
        0
    ) as is_pickup_data_usable,

    ifNull(
        was_fresh_at_load = 1
        and is_fresh_now = 1
        and is_installed = 1
        and is_returning = 1
        and isNotNull(num_docks_available),
        0
    ) as is_return_data_usable

from freshness
