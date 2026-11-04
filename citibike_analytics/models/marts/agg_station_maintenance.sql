with tickets as (

    select *
    from {{ ref('fct_maintenance_tickets') }}

)

select
    station_code,
    station_id,
    station_name,
    region_id,
    region_name,
    station_mapping_status,

    count() as total_tickets,

    countIf(status = 'Open') as open_tickets,

    countIf(status = 'In Progress') as in_progress_tickets,

    countIf(is_resolved = 1) as resolved_tickets,

    countIf(is_backlog = 1) as backlog_tickets,

    countIf(
        is_backlog = 1
        and priority = 'high'
    ) as high_priority_backlog_tickets,

    count(resolution_hours) as tickets_with_resolution_time,

    avgOrNull(resolution_hours) as avg_resolution_hours,

    count(repair_cost_usd) as tickets_with_recorded_cost,

    countIf(
        isNull(repair_cost_usd)
    ) as tickets_missing_cost,

    sumOrNull(repair_cost_usd) as recorded_repair_cost_usd,

    max(extracted_at) as data_extracted_at

from tickets

group by
    station_code,
    station_id,
    station_name,
    region_id,
    region_name,
    station_mapping_status