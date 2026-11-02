-- A missing input is not a zero score or evidence that no action is needed.
select *
from {{ ref('mart_station_rebalancing_priority') }}
where (is_pickup_priority_eligible = 0 and isNotNull(add_bikes_score))
   or (is_return_priority_eligible = 0 and isNotNull(return_capacity_score))
   or (is_pickup_priority_eligible = 1 and isNull(add_bikes_score))
   or (is_return_priority_eligible = 1 and isNull(return_capacity_score))
   or (isNull(add_bikes_score) and isNull(return_capacity_score) and isNotNull(priority_score))
   or ((isNotNull(add_bikes_score) or isNotNull(return_capacity_score)) and isNull(priority_score))
   or priority_score < 0 or priority_score > 100
   or add_bikes_score < 0 or add_bikes_score > 100
   or return_capacity_score < 0 or return_capacity_score > 100
   or priority_score != greatest(ifNull(add_bikes_score, 0), ifNull(return_capacity_score, 0))
   or zero_bike_share_last_6h < 0 or zero_bike_share_last_6h > 1
   or zero_dock_share_last_6h < 0 or zero_dock_share_last_6h > 1
   or (pickup_observations_last_6h = 0 and isNotNull(zero_bike_share_last_6h))
   or (return_observations_last_6h = 0 and isNotNull(zero_dock_share_last_6h))
   or (pickup_observations_last_6h > 0 and isNull(zero_bike_share_last_6h))
   or (return_observations_last_6h > 0 and isNull(zero_dock_share_last_6h))
   or (assessment_status = 'COMPLETE' and (
       is_pickup_priority_eligible != 1 or is_return_priority_eligible != 1))
   or (assessment_status = 'PARTIAL_DATA' and
       is_pickup_priority_eligible + is_return_priority_eligible != 1)
   or (recommended_action = 'NO_ACTION' and (
       assessment_status != 'COMPLETE'
       or isNull(add_bikes_score) or isNull(return_capacity_score)
       or add_bikes_score != 0 or return_capacity_score != 0))
   or (recommended_action = 'CHECK_BOTH_SIDES' and (
       isNull(add_bikes_score) or isNull(return_capacity_score)
       or add_bikes_score <= 0 or return_capacity_score <= 0))
   or (isNull(priority_score) and priority_band != 'NOT_RANKED')
