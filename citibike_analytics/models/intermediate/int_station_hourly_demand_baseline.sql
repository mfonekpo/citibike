{{ config(materialized='table') }}

{% set analytics_timezone = var(
    'analytics_timezone', 'America/New_York'
) %}

-- Discover all months available in the Bronze trip dataset.
with reporting_months as (

    select distinct
        toDate(toStartOfMonth(release_month))
            as reporting_start_date

    from {{ ref('stg_citibike__raw_citibike_tripdata') }}

    where release_month is not null

),

-- Calculate the boundaries for each reporting month.
bounds as (

    select
        reporting_start_date,

        addMonths(reporting_start_date, 1)
            as reporting_end_date_exclusive,

        toTimeZone(
            toDateTime(
                concat(
                    toString(reporting_start_date),
                    ' 00:00:00'
                ),
                '{{ analytics_timezone }}'
            ),
            'UTC'
        ) as window_start_utc,

        toTimeZone(
            toDateTime(
                concat(
                    toString(
                        addMonths(reporting_start_date, 1)
                    ),
                    ' 00:00:00'
                ),
                '{{ analytics_timezone }}'
            ),
            'UTC'
        ) as window_end_utc

    from reporting_months

),

-- Generate every actual UTC hour in each reporting month.
-- This preserves daylight-saving transitions.
calendar_hours as (

    select
        reporting_start_date,

        window_start_utc
            + toIntervalHour(hour_offset)
            as hour_start_utc

    from bounds

    array join range(
        toUInt32(
            greatest(
                dateDiff(
                    'hour',
                    window_start_utc,
                    window_end_utc
                ),
                0
            )
        )
    ) as hour_offset

),

-- Count the calendar opportunities for each weekday/hour.
opportunities as (

    select
        reporting_start_date,

        toDayOfWeek(
            toTimeZone(
                hour_start_utc,
                '{{ analytics_timezone }}'
            )
        ) as local_weekday,

        toHour(
            toTimeZone(
                hour_start_utc,
                '{{ analytics_timezone }}'
            )
        ) as local_hour,

        uniqExact(
            toDate(
                toTimeZone(
                    hour_start_utc,
                    '{{ analytics_timezone }}'
                )
            )
        ) as baseline_days,

        count() as baseline_hour_opportunities

    from calendar_hours

    group by
        reporting_start_date,
        local_weekday,
        local_hour

),

-- Generate the complete 7-day x 24-hour grid.
weekday_hours as (

    select
        toUInt8(intDiv(number, 24) + 1)
            as local_weekday,

        toUInt8(number % 24)
            as local_hour

    from numbers(168)

),

-- Associate each hourly trip aggregate with its local
-- calendar month.
reporting_usage as (

    select
        m.reporting_start_date,

        u.station_id,

        toDayOfWeek(
            toTimeZone(
                u.hour_start_utc,
                '{{ analytics_timezone }}'
            )
        ) as local_weekday,

        toHour(
            toTimeZone(
                u.hour_start_utc,
                '{{ analytics_timezone }}'
            )
        ) as local_hour,

        u.departures,
        u.arrivals

    from {{ ref('agg_station_hourly_usage') }} as u

    inner join reporting_months as m
        on m.reporting_start_date = toStartOfMonth(
            toDate(
                toTimeZone(
                    u.hour_start_utc,
                    '{{ analytics_timezone }}'
                )
            )
        )

),

-- Aggregate trip activity by month, station, weekday and hour.
station_totals as (

    select
        reporting_start_date,
        station_id,
        local_weekday,
        local_hour,

        sum(departures) as total_departures,
        sum(arrivals) as total_arrivals

    from reporting_usage

    group by
        reporting_start_date,
        station_id,
        local_weekday,
        local_hour

),

-- Identify stations with observed trip activity
-- within each reporting month.
observed_stations as (

    select
        reporting_start_date,
        station_id,

        toUInt8(1) as station_in_reporting_data

    from reporting_usage

    group by
        reporting_start_date,
        station_id

    having sum(departures) + sum(arrivals) > 0

),

stations as (

    select distinct
        assumeNotNull(station_id) as station_id

    from {{ ref('dim_stations') }}

    where station_id is not null
      and station_id != ''

),

-- Produce one record per reporting month, station,
-- weekday and hour, including zero-trip periods.
complete_baseline as (

    select
        m.reporting_start_date
            as reporting_start_date,

        addMonths(m.reporting_start_date, 1)
            as reporting_end_date_exclusive,

        s.station_id as station_id,

        h.local_weekday as local_weekday,
        h.local_hour as local_hour,

        coalesce(o.baseline_days, 0)
            as baseline_days,

        coalesce(o.baseline_hour_opportunities, 0)
            as baseline_hour_opportunities,

        coalesce(t.total_departures, 0)
            as total_departures,

        coalesce(t.total_arrivals, 0)
            as total_arrivals,

        coalesce(seen.station_in_reporting_data, 0)
            as station_in_reporting_data

    from reporting_months as m

    cross join stations as s

    cross join weekday_hours as h

    left join opportunities as o
        on m.reporting_start_date = o.reporting_start_date
        and h.local_weekday = o.local_weekday
        and h.local_hour = o.local_hour

    left join station_totals as t
        on m.reporting_start_date = t.reporting_start_date
        and s.station_id = t.station_id
        and h.local_weekday = t.local_weekday
        and h.local_hour = t.local_hour

    left join observed_stations as seen
        on m.reporting_start_date = seen.reporting_start_date
        and s.station_id = seen.station_id

)

select
    complete_baseline.*,

    if(
        station_in_reporting_data = 1,

        total_departures / nullIf(
            toFloat64(baseline_hour_opportunities),
            0
        ),

        cast(null as Nullable(Float64))
    ) as avg_departures,

    if(
        station_in_reporting_data = 1,

        total_arrivals / nullIf(
            toFloat64(baseline_hour_opportunities),
            0
        ),

        cast(null as Nullable(Float64))
    ) as avg_arrivals,

    if(
        station_in_reporting_data = 1,

        'reporting_calendar_assuming_station_operational',

        'no_mapped_trips_observed_operating_history_unknown'
    ) as coverage_basis

from complete_baseline