{{ config(materialized='view') }}

-- One row per station. This is an advisory heuristic, not a demand forecast:
-- 70% current shortage, 20% historical hourly activity, 10% recent zero-stock
-- observations. Missing inputs are unknown, not evidence of zero demand/risk.
with baseline as (
    select
        station_id,
        local_weekday,
        local_hour,
        toNullable(baseline_days) as baseline_days,
        toNullable(baseline_hour_opportunities) as baseline_hour_opportunities,
        toNullable(avg_departures) as avg_departures,
        toNullable(avg_arrivals) as avg_arrivals,
        toNullable(reporting_start_date) as baseline_start_date,
        toNullable(reporting_end_date_exclusive) as baseline_end_date_exclusive,
        toNullable(station_in_reporting_data) as station_in_reporting_data
    from {{ ref('int_station_hourly_demand_baseline') }}
),

recent_availability as (
    select
        station_id,
        sum(pickup_usable_observations) as pickup_observations_last_6h,
        sum(return_usable_observations) as return_observations_last_6h,
        sum(zero_bike_observations) as zero_bike_observations_last_6h,
        sum(zero_dock_observations) as zero_dock_observations_last_6h
    from {{ ref('agg_station_hourly_availability') }}
    -- Six completed UTC hours. The current partial hour is excluded.
    where hour_start_utc >= toStartOfHour(now('UTC')) - interval 6 hour
      and hour_start_utc < toStartOfHour(now('UTC'))
    group by station_id
),

features as (
    select
        a.station_id as station_id,
        a.station_code as station_code,
        a.station_name as station_name,
        a.region_id as region_id,
        a.region_name as region_name,
        a.latitude as latitude,
        a.longitude as longitude,
        a.station_capacity as station_capacity,
        a.feed_updated_at as feed_updated_at,
        a.last_reported_at as last_reported_at,
        a.evaluated_at as evaluated_at,
        a.is_installed as is_installed,
        a.is_fresh_now as is_fresh_now,
        a.was_fresh_at_load as was_fresh_at_load,
        a.has_station_metadata as has_station_metadata,
        a.is_pickup_data_usable as is_pickup_data_usable,
        a.is_return_data_usable as is_return_data_usable,
        a.num_bikes_available as num_bikes_available,
        a.num_docks_available as num_docks_available,
        toDayOfWeek(toTimeZone(a.evaluated_at,
            '{{ var("analytics_timezone", "America/New_York") }}')) as local_weekday,
        toHour(toTimeZone(a.evaluated_at,
            '{{ var("analytics_timezone", "America/New_York") }}')) as local_hour,
        b.baseline_days,
        b.baseline_hour_opportunities,
        b.baseline_start_date,
        b.baseline_end_date_exclusive,
        b.avg_departures as historical_avg_departures,
        b.avg_arrivals as historical_avg_arrivals,
        ifNull(b.station_in_reporting_data, 0) as station_in_reporting_data,
        ifNull(h.pickup_observations_last_6h, 0) as pickup_observations_last_6h,
        ifNull(h.return_observations_last_6h, 0) as return_observations_last_6h,
        ifNull(h.zero_bike_observations_last_6h, 0)
            / nullIf(toFloat64(ifNull(h.pickup_observations_last_6h, 0)), 0)
            as zero_bike_share_last_6h,
        ifNull(h.zero_dock_observations_last_6h, 0)
            / nullIf(toFloat64(ifNull(h.return_observations_last_6h, 0)), 0)
            as zero_dock_share_last_6h,
        least(
            toFloat64(ifNull(a.station_capacity, 0)),
            greatest(2.0, ceil(0.20 * toFloat64(ifNull(a.station_capacity, 0))))
        ) as low_inventory_threshold,
        toUInt32({{ var('minimum_baseline_days', 3) }}) as minimum_baseline_days,
        toFloat64({{ var('priority_reference_trips_per_hour', 20) }})
            as priority_reference_trips_per_hour
    from {{ ref('mart_station_latest_availability') }} as a
    left join baseline as b
        on a.station_id = b.station_id
        and b.local_weekday = toDayOfWeek(toTimeZone(a.evaluated_at,
            '{{ var("analytics_timezone", "America/New_York") }}'))
        and b.local_hour = toHour(toTimeZone(a.evaluated_at,
            '{{ var("analytics_timezone", "America/New_York") }}'))
    left join recent_availability as h on a.station_id = h.station_id
),

eligibility as (
    select
        *,
        ifNull(
            station_in_reporting_data = 1
            and baseline_days >= minimum_baseline_days
            and isNotNull(historical_avg_departures)
            and isNotNull(historical_avg_arrivals), 0
        ) as has_supported_baseline,
        ifNull(
            has_station_metadata = 1 and station_capacity > 0
            and is_pickup_data_usable = 1
            and has_supported_baseline = 1
            and pickup_observations_last_6h > 0, 0
        ) as is_pickup_priority_eligible,
        ifNull(
            has_station_metadata = 1 and station_capacity > 0
            and is_return_data_usable = 1
            and has_supported_baseline = 1
            and return_observations_last_6h > 0, 0
        ) as is_return_priority_eligible
    from features
),

scored as (
    select
        *,
        if(is_pickup_priority_eligible = 0, cast(NULL as Nullable(Float64)),
            if(num_bikes_available >= low_inventory_threshold, 0.0,
                round(
                    70.0 * (low_inventory_threshold - num_bikes_available)
                        / greatest(low_inventory_threshold, 1.0)
                    + 20.0 * least(1.0, historical_avg_departures
                        / priority_reference_trips_per_hour)
                    + 10.0 * zero_bike_share_last_6h, 2
                )
            )
        ) as add_bikes_score,
        if(is_return_priority_eligible = 0, cast(NULL as Nullable(Float64)),
            if(num_docks_available >= low_inventory_threshold, 0.0,
                round(
                    70.0 * (low_inventory_threshold - num_docks_available)
                        / greatest(low_inventory_threshold, 1.0)
                    + 20.0 * least(1.0, historical_avg_arrivals
                        / priority_reference_trips_per_hour)
                    + 10.0 * zero_dock_share_last_6h, 2
                )
            )
        ) as return_capacity_score
    from eligibility
),

prioritized as (
    select
        *,
        if(isNull(add_bikes_score) and isNull(return_capacity_score),
            cast(NULL as Nullable(Float64)),
            greatest(ifNull(add_bikes_score, 0), ifNull(return_capacity_score, 0))
        ) as priority_score,
        multiIf(
            has_station_metadata = 0 or ifNull(station_capacity, 0) = 0,
                'INSUFFICIENT_METADATA',
            is_fresh_now = 0 or was_fresh_at_load = 0, 'STALE_STATUS',
            ifNull(is_installed, 0) != 1, 'OUT_OF_SERVICE',
            is_pickup_data_usable = 0 and is_return_data_usable = 0, 'STATUS_UNUSABLE',
            has_supported_baseline = 0, 'INSUFFICIENT_BASELINE',
            is_pickup_priority_eligible = 1 and is_return_priority_eligible = 1, 'COMPLETE',
            is_pickup_priority_eligible = 1 or is_return_priority_eligible = 1, 'PARTIAL_DATA',
            'INSUFFICIENT_HISTORY'
        ) as assessment_status
    from scored
),

actions as (
    select
        *,
        multiIf(
            add_bikes_score > 0 and return_capacity_score > 0, 'CHECK_BOTH_SIDES',
            add_bikes_score > 0, 'ADD_BIKES',
            return_capacity_score > 0, 'RESTORE_RETURN_CAPACITY',
            assessment_status = 'COMPLETE', 'NO_ACTION',
            assessment_status = 'PARTIAL_DATA', 'CHECK_UNASSESSED_SIDE',
            'INSUFFICIENT_DATA'
        ) as recommended_action
    from prioritized
)

select
    *,
    multiIf(
        isNull(priority_score), 'NOT_RANKED',
        priority_score = 0, 'NOT_RANKED',
        priority_score >= 70, 'HIGH',
        priority_score >= 35, 'MEDIUM',
        'LOW'
    ) as priority_band
from actions
