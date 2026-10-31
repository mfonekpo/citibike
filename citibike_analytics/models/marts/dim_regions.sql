with regions as (
    select
        region_id,
        region_name,
        feed_updated_at,
        loaded_at
    from {{ ref('stg_citibike__stream_system_regions') }}
),


ranked as (
    select
        region_id,
        region_name,
        feed_updated_at,
        loaded_at,

        row_number() over(
            partition by region_id
            order BY
                feed_updated_at desc nulls last,
                loaded_at desc
        ) as row_num
    from regions
),

final as (
    select
        region_id,
        region_name,
        feed_updated_at,
        loaded_at
    from ranked
    where row_num = 1
)

select * from final