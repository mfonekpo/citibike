-- Replayed CSV loads must not silently inflate station demand.
select ride_id, count() as row_count
from {{ ref('fct_trips') }}
group by ride_id
having ride_id is null or ride_id = '' or count() > 1
