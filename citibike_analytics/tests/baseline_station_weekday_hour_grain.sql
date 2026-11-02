-- One row per known station and local weekday/hour, including zero-trip slots.
select station_id
from {{ ref('int_station_hourly_demand_baseline') }}
group by station_id
having count() != 168
    or uniqExact(tuple(local_weekday, local_hour)) != 168
    or min(local_weekday) != 1
    or max(local_weekday) != 7
    or min(local_hour) != 0
    or max(local_hour) != 23
