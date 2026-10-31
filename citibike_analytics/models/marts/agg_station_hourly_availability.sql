{{ config(materialized='view') }}

-- Repeated polls of one source update describe one station observation.
-- Keep its first arrival; conflicting contents are counted but not used in metrics.
with ranked as (
    select
        assumeNotNull(a.station_id) as station_id,
        assumeNotNull(a.feed_updated_at) as feed_updated_at,
        last_reported_at,
        loaded_at,
        snapshot_id,
        source_id,
        num_bikes_available,
        num_ebikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        row_number() over (
            partition by a.source_id, a.station_id, a.feed_updated_at
            order by loaded_at asc,
                     snapshot_id asc nulls last
        ) as row_num,
        uniqExact(tuple(
            last_reported_at,
            num_bikes_available,
            num_ebikes_available,
            num_bikes_disabled,
            num_docks_available,
            num_docks_disabled,
            is_installed,
            is_renting,
            is_returning
        )) over (
            partition by a.source_id, a.station_id, a.feed_updated_at
        ) as observation_version_count
    from {{ ref('fct_station_availability') }} as a
    where isNotNull(a.station_id)
      and isNotNull(a.feed_updated_at)
),

observations as (
    select
        *,
        ifNull(
            observation_version_count = 1
            and
            (feed_updated_at between loaded_at - interval {{ var('max_report_age_seconds', 300) }} second
                                 and loaded_at + interval {{ var('allowed_future_clock_skew_seconds', 60) }} second)
            and
            (last_reported_at between loaded_at - interval {{ var('max_report_age_seconds', 300) }} second
                                  and loaded_at + interval {{ var('allowed_future_clock_skew_seconds', 60) }} second),
            0
        ) as was_fresh_at_load
    from ranked
    where row_num = 1
),

qualified as (
    select
        *,
        toStartOfHour(feed_updated_at) as hour_start_utc,

        ifNull(
            was_fresh_at_load = 1
            and is_installed = 1
            and is_renting = 1
            and isNotNull(num_bikes_available),
            0
        ) as pickup_observation_usable,

        ifNull(
            was_fresh_at_load = 1
            and is_installed = 1
            and is_returning = 1
            and isNotNull(num_docks_available),
            0
        ) as return_observation_usable

    from observations
),

hourly as (
    select
        station_id,
        hour_start_utc,

        count() as observation_count,
        countIf(observation_version_count > 1) as conflicting_observations,
        uniqExact(toStartOfMinute(feed_updated_at)) as observed_minute_buckets,
        min(feed_updated_at) as first_observed_at,
        max(feed_updated_at) as last_observed_at,

        countIf(was_fresh_at_load = 1) as fresh_at_load_observations,
        countIf(pickup_observation_usable = 1) as pickup_usable_observations,
        countIf(return_observation_usable = 1) as return_usable_observations,

        countIf(pickup_observation_usable = 1
                and num_bikes_available = 0) as zero_bike_observations,
        countIf(return_observation_usable = 1
                and num_docks_available = 0) as zero_dock_observations,
        avgIf(toFloat64(ifNull(num_bikes_available, 0)),
              pickup_observation_usable = 1) as avg_bikes_raw,
        avgIf(toFloat64(ifNull(num_docks_available, 0)),
              return_observation_usable = 1) as avg_docks_raw,
        minIf(ifNull(num_bikes_available, 0),
              pickup_observation_usable = 1) as min_bikes_raw,
        minIf(ifNull(num_docks_available, 0),
              return_observation_usable = 1) as min_docks_raw
    from qualified

    group by station_id, hour_start_utc
)

select
    station_id,
    hour_start_utc,
    toDate(toTimeZone(hour_start_utc, '{{ var("analytics_timezone", "America/New_York") }}')) as local_date,
    toHour(toTimeZone(hour_start_utc, '{{ var("analytics_timezone", "America/New_York") }}')) as local_hour,
    observation_count,
    conflicting_observations,
    observed_minute_buckets,

    round(observed_minute_buckets / 60.0, 4) as observed_minute_bucket_share,

    first_observed_at,
    last_observed_at,
    fresh_at_load_observations,

    round(fresh_at_load_observations / observation_count, 4) as fresh_at_load_share,

    pickup_usable_observations,
    return_usable_observations,
    zero_bike_observations,
    zero_dock_observations,


    if(pickup_usable_observations = 0,
       cast(null as Nullable(Float64)),
       toNullable(round(avg_bikes_raw, 2))) as avg_bikes_available,

    if(return_usable_observations = 0,
       cast(null as Nullable(Float64)),
       toNullable(round(avg_docks_raw, 2))) as avg_docks_available,

    if(pickup_usable_observations = 0,
       cast(null as Nullable(UInt32)),
       toNullable(min_bikes_raw)) as min_bikes_available,

    if(return_usable_observations = 0,
       cast(null as Nullable(UInt32)),
       toNullable(min_docks_raw)) as min_docks_available,

    if(pickup_usable_observations = 0,
       cast(null as Nullable(Float64)),
       toNullable(round(zero_bike_observations /
                        toFloat64(pickup_usable_observations), 4)))
       as zero_bike_observation_share,

    if(return_usable_observations = 0,
       cast(null as Nullable(Float64)),
       toNullable(round(zero_dock_observations /
                        toFloat64(return_usable_observations), 4)))
       as zero_dock_observation_share

from hourly
