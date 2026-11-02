-- depends_on: {{ ref('mart_station_rebalancing_priority') }}
-- Return one failure row for an invalid analytics configuration.
select 'invalid_gold_configuration' as failure
where toDate('{{ var("trip_reporting_start_date", "2026-09-01") }}')
        >= toDate('{{ var("trip_reporting_end_date", "2026-10-01") }}')
    or {{ var('minimum_baseline_days', 3) }} < 1
    or {{ var('priority_reference_trips_per_hour', 20) }} <= 0
    or {{ var('max_report_age_seconds', 300) }} < 0
    or {{ var('allowed_future_clock_skew_seconds', 60) }} < 0
