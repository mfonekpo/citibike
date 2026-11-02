-- One row describes one station in one UTC hour, regardless of source count.
select station_id, hour_start_utc, count() as row_count
from {{ ref('agg_station_hourly_availability') }}
group by station_id, hour_start_utc
having row_count > 1
    or isNull(station_id)
    or isNull(hour_start_utc)
