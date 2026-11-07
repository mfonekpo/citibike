-- Each station must have 168 unique weekday/hour
-- combinations for every reporting month.

select
    reporting_start_date,
    station_id,
    count() as row_count

from {{ ref('int_station_hourly_demand_baseline') }}

group by
    reporting_start_date,
    station_id

having count() != 168

    or uniqExact(
        tuple(local_weekday, local_hour)
    ) != 168

    or min(local_weekday) != 1
    or max(local_weekday) != 7

    or min(local_hour) != 0
    or max(local_hour) != 23