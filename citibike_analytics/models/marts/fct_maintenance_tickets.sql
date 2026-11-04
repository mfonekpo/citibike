with tickets as (

    select *
    from {{ ref('stg_citibike__baserow_maintenance_tickets') }}

),

stations as (

    select
        station_code,
        nullIf(station_id, '') as station_id,
        nullIf(station_name, '') as station_name,
        nullIf(region_id, '') as region_id,
        nullIf(region_name, '') as region_name

    from {{ ref('dim_stations') }}

),

enriched as (

    select
        t.*,

        s.station_id,
        s.station_name,
        s.region_id,
        s.region_name

    from tickets as t

    left join stations as s
        on t.station_code = s.station_code

)

select
    enriched.*,

    multiIf(
        isNull(station_code), 'missing_code',
        isNull(station_id), 'unmatched',
        'matched'
    ) as station_mapping_status,

    ifNull(
        status in ('Open', 'In Progress'),
        0
    ) as is_backlog,

    ifNull(
        status = 'Resolved',
        0
    ) as is_resolved,

    case
        when status = 'Resolved'
            and reported_at is not null
            and resolved_at is not null
            and resolved_at >= reported_at
        then
            dateDiff(
                'millisecond',
                reported_at,
                resolved_at
            ) / 3600000.0
        else null
    end as resolution_hours

from enriched