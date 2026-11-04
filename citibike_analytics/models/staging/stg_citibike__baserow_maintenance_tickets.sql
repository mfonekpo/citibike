with source as (

    SELECT
        *
    from {{ source('citibike_raw', 'baserow_maintenance_tickets_raw') }}
),


unpacked as (
    select
        baserow_table_id,
        baserow_row_id,
        extraction_id,
        extracted_at,
        loaded_at,
        JSONExtractString(raw_payload, 'ticket_id') as ticket_id,
        JSONExtractFloat(raw_payload, 'station_code') as station_code,
        JSONExtractString(raw_payload, 'issue_type', 'value') as issue_type,
        JSONExtractString(raw_payload, 'status', 'value') as status,

        parseDateTimeBestEffortOrNull(
            JSONExtractString(raw_payload, 'reported_at')
            ) as reported_at,

        parseDateTimeBestEffortOrNull(
            JSONExtractString(raw_payload, 'resolved_at')
            ) as resolved_at,
        
        JSONExtractString(raw_payload, 'priority') as priority,

        toDecimal64OrNull(
            JSONExtractString(raw_payload, 'repair_cost_usd'),
            2
        ) as repair_cost_usd

    from source
)

select * from unpacked