select
    ticket_id,
    status,
    reported_at,
    resolved_at,
    repair_cost_usd

from {{ ref('fct_maintenance_tickets') }}

where reported_at is null
    or repair_cost_usd < 0
    or resolved_at < reported_at
    or (
        status = 'Resolved'
        and resolved_at is null
    )
    or (
        status in ('Open', 'In Progress')
        and resolved_at is not null
    )