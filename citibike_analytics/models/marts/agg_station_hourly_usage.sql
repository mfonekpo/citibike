with departures as (
    select
        assumeNotNull(t.start_station_id) as station_id,
        toStartOfHour(assumeNotNull(t.started_at)) as hour_start_utc,

        count() as departures,
        toUInt64(0) as arrivals
    from {{ ref('fct_trips') }} as t
    where isNotNull(t.start_station_id)
      and isNotNull(t.started_at)
    group by station_id, hour_start_utc
),

arrivals as (
    select
        assumeNotNull(t.end_station_id) as station_id,
        toStartOfHour(assumeNotNull(t.ended_at)) as hour_start_utc,

        toUInt64(0) as departures,
        count() as arrivals
    from {{ ref('fct_trips') }} as t
    where isNotNull(t.end_station_id)
      and isNotNull(t.ended_at)
    group by station_id, hour_start_utc
),

station_events as (
    select * from departures
    union all
    select * from arrivals
),

hourly as (
    select
        station_id,
        hour_start_utc,
        sum(departures) as departures,
        sum(arrivals) as arrivals
    from station_events
    group by station_id, hour_start_utc
)

select
    station_id,
    hour_start_utc,

    toDate(toTimeZone(hour_start_utc, 'America/New_York')) as local_date,
    toHour(toTimeZone(hour_start_utc, 'America/New_York')) as local_hour,

    departures,
    arrivals,
    toInt64(departures) - toInt64(arrivals) as net_outflow,
    departures + arrivals as station_movements

from hourly