with latest_completed_extraction as (

    select
        baserow_table_id,
        extraction_id
    from {{ source('citibike_raw', 'baserow_completed_extractions') }}
    where baserow_table_id = {{ var('baserow_maintenance_table_id') }}
    order by
        extracted_at desc,
        extraction_id desc,
        completed_at desc
    limit 1

),

source as (

    select
        r.*
    from {{ source('citibike_raw', 'baserow_maintenance_tickets_raw') }} as r

    inner join latest_completed_extraction as completed
        on r.baserow_table_id = completed.baserow_table_id
        and r.extraction_id = completed.extraction_id

),

unpacked as (

    select
        baserow_table_id,
        baserow_row_id,
        extraction_id,
        extracted_at,
        loaded_at,

        nullIf(
            trimBoth(JSONExtractString(raw_payload, 'ticket_id')),
            ''
        ) as ticket_id,

        nullIf(
            trimBoth(JSONExtractString(raw_payload, 'station_code')),
            ''
        ) as station_code,

        nullIf(
            trimBoth(JSONExtractString(raw_payload, 'issue_type', 'value')),
            ''
        ) as issue_type,

        nullIf(
            trimBoth(JSONExtractString(raw_payload, 'status', 'value')),
            ''
        ) as status,

        nullIf(
            trimBoth(JSONExtractString(raw_payload, 'priority', 'value')),
            ''
        ) as priority,

        parseDateTime64BestEffortOrNull(
            trimBoth(JSONExtractString(raw_payload, 'reported_at')),
            3,
            'UTC'
        ) as reported_at,

        parseDateTime64BestEffortOrNull(
            trimBoth(JSONExtractString(raw_payload, 'resolved_at')),
            3,
            'UTC'
        ) as resolved_at,

        toDecimal64OrNull(
            trimBoth(JSONExtractString(raw_payload, 'repair_cost_usd')),
            2
        ) as repair_cost_usd

    from source

)

select * from unpacked