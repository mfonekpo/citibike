{{ config(materialized='table') }}

select
    *
from {{ ref('int_trip_validation') }}
where validation_status = 'REJECTED'