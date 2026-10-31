{{ config(materialized='table') }}

{% set analytics_timezone = var('analytics_timezone', 'America/New_York') %}
{% set reporting_start = var('trip_reporting_start_date', '2026-09-01') %}
{% set reporting_end = var('trip_reporting_end_date', '2026-10-01') %}

with bounds as (
    select
        toTimeZone(toDateTime('{{ reporting_start }} 00:00:00', '{{ analytics_timezone }}'), 'UTC') as window_start_utc,
        toTimeZone(toDateTime('{{ reporting_end }} 00:00:00', '{{ analytics_timezone }}'), 'UTC') as window_end_utc
),

calendar_hours as (
    select
        window_start_utc + toIntervalHour(hour_offset) as hour_start_utc
    from bounds
    -- Enumerating actual UTC hours preserves both fall-back hours and skips
    -- the nonexistent spring-forward hour in the local reporting calendar.
    array join range(toUInt32(greatest(dateDiff('hour', window_start_utc, window_end_utc), 0))) as hour_offset
),

opportunities as (
    select
        toDayOfWeek(toTimeZone(hour_start_utc, '{{ analytics_timezone }}')) as local_weekday,
        toHour(toTimeZone(hour_start_utc, '{{ analytics_timezone }}')) as local_hour,
        uniqExact(toDate(toTimeZone(hour_start_utc, '{{ analytics_timezone }}'))) as baseline_days,
        count() as baseline_hour_opportunities
    from calendar_hours
    group by local_weekday, local_hour
),

weekday_hours as (
    select
        toUInt8(intDiv(number, 24) + 1) as local_weekday,
        toUInt8(number % 24) as local_hour
    from numbers(168)
),

reporting_usage as (
    select
        u.station_id,
        toDayOfWeek(toTimeZone(u.hour_start_utc, '{{ analytics_timezone }}')) as local_weekday,
        toHour(toTimeZone(u.hour_start_utc, '{{ analytics_timezone }}')) as local_hour,
        u.departures,
        u.arrivals
    from {{ ref('agg_station_hourly_usage') }} as u
    cross join bounds as b
    where u.hour_start_utc >= b.window_start_utc
      and u.hour_start_utc < b.window_end_utc
),

station_totals as (
    select
        station_id,
        local_weekday,
        local_hour,
        sum(departures) as total_departures,
        sum(arrivals) as total_arrivals
    from reporting_usage
    group by station_id, local_weekday, local_hour
),

observed_stations as (
    select station_id, toUInt8(1) as station_in_reporting_data
    from reporting_usage
    group by station_id
    having sum(departures) + sum(arrivals) > 0
),

stations as (
    select distinct assumeNotNull(station_id) as station_id
    from {{ ref('dim_stations') }}
    where station_id is not null and station_id != ''
),

complete_baseline as (
    select
        s.station_id as station_id,
        h.local_weekday as local_weekday,
        h.local_hour as local_hour,
        coalesce(o.baseline_days, 0) as baseline_days,
        coalesce(o.baseline_hour_opportunities, 0) as baseline_hour_opportunities,
        coalesce(t.total_departures, 0) as total_departures,
        coalesce(t.total_arrivals, 0) as total_arrivals,
        coalesce(seen.station_in_reporting_data, 0) as station_in_reporting_data
    from stations as s
    cross join weekday_hours as h
    left join opportunities as o
        on h.local_weekday = o.local_weekday and h.local_hour = o.local_hour
    left join station_totals as t
        on s.station_id = t.station_id
        and h.local_weekday = t.local_weekday
        and h.local_hour = t.local_hour
    left join observed_stations as seen
        on s.station_id = seen.station_id
)

select
    complete_baseline.*,
    -- Zero-trip dates belong in the denominator. For an observed station we
    -- assume operation throughout this period; historical opening dates are
    -- unavailable. A station never observed in this period has unknown demand.
    if(
        station_in_reporting_data = 1,
        total_departures / nullIf(toFloat64(baseline_hour_opportunities), 0),
        cast(null as Nullable(Float64))
    ) as avg_departures,
    if(
        station_in_reporting_data = 1,
        total_arrivals / nullIf(toFloat64(baseline_hour_opportunities), 0),
        cast(null as Nullable(Float64))
    ) as avg_arrivals,
    if(
        station_in_reporting_data = 1,
        'reporting_calendar_assuming_station_operational',
        'no_mapped_trips_observed_operating_history_unknown'
    ) as coverage_basis,
    toDate('{{ reporting_start }}') as reporting_start_date,
    toDate('{{ reporting_end }}') as reporting_end_date_exclusive
from complete_baseline
