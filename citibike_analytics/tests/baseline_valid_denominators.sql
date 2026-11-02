select *
from {{ ref('int_station_hourly_demand_baseline') }}
where reporting_end_date_exclusive <= reporting_start_date
   or baseline_hour_opportunities < baseline_days
   or (baseline_hour_opportunities = 0 and
       (avg_departures is not null or avg_arrivals is not null))
   or (station_in_reporting_data = 0 and
       (avg_departures is not null or avg_arrivals is not null))
   or (station_in_reporting_data = 1 and baseline_hour_opportunities > 0 and
       (avg_departures is null or avg_arrivals is null
        or abs(avg_departures - total_departures / baseline_hour_opportunities) > 0.000001
        or abs(avg_arrivals - total_arrivals / baseline_hour_opportunities) > 0.000001))
