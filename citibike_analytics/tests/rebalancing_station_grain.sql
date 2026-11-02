select station_id, count() as row_count
from {{ ref('mart_station_rebalancing_priority') }}
group by station_id
having row_count > 1 or isNull(station_id) or station_id = ''
