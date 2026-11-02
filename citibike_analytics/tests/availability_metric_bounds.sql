-- Unknown sides stay NULL; conflicting source updates cannot be usable.
select *
from {{ ref('agg_station_hourly_availability') }}
where observation_count = 0
   or conflicting_observations > observation_count
   or fresh_at_load_observations > observation_count - conflicting_observations
   or pickup_usable_observations > fresh_at_load_observations
   or return_usable_observations > fresh_at_load_observations
   or zero_bike_observations > pickup_usable_observations
   or zero_dock_observations > return_usable_observations
   or observed_minute_buckets > 60
   or observed_minute_bucket_share < 0 or observed_minute_bucket_share > 1
   or fresh_at_load_share < 0 or fresh_at_load_share > 1
   or zero_bike_observation_share < 0 or zero_bike_observation_share > 1
   or zero_dock_observation_share < 0 or zero_dock_observation_share > 1
   or (pickup_usable_observations = 0 and (
       isNotNull(avg_bikes_available)
       or isNotNull(min_bikes_available)
       or isNotNull(zero_bike_observation_share)))
   or (return_usable_observations = 0 and (
       isNotNull(avg_docks_available)
       or isNotNull(min_docks_available)
       or isNotNull(zero_dock_observation_share)))
   or (pickup_usable_observations > 0 and (
       isNull(avg_bikes_available)
       or isNull(min_bikes_available)
       or isNull(zero_bike_observation_share)))
   or (return_usable_observations > 0 and (
       isNull(avg_docks_available)
       or isNull(min_docks_available)
       or isNull(zero_dock_observation_share)))
