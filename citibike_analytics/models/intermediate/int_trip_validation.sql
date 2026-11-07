{{ config(materialized='table') }}

-- STEP 1: Read the cleaned trips and generate a fingerprint
-- for comparing records with the same ride_id.

with source_data as (

    select
        *,

        dateDiff(
            'millisecond',
            started_at,
            ended_at
        ) / 1000.0 as duration_seconds

    from {{ ref('stg_citibike__raw_citibike_tripdata') }}

),


-- STEP 2: Apply the rejection rules in the data contract.

validated as (

    select
        *,

        arrayFilter(
            code -> code != '',

            [
                if(
                    isNull(ride_id),
                    'MISSING_RIDE_ID',
                    ''
                ),

                if(
                    isNull(started_at)
                    or not match(
                        trimBoth(raw_started_at),
                        '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?$'
                    ),
                    'INVALID_START_TIMESTAMP',
                    ''
                ),

                if(
                    isNull(ended_at)
                    or not match(
                        trimBoth(raw_ended_at),
                        '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?$'
                    ),
                    'INVALID_END_TIMESTAMP',
                    ''
                ),

                if(
                    isNotNull(started_at)
                    and isNotNull(ended_at)
                    and ended_at <= started_at,
                    'INVALID_TRIP_TIME',
                    ''
                ),

                if(
                    ifNull(rideable_type, '') not in (
                        'classic_bike',
                        'electric_bike'
                    ),
                    'INVALID_RIDEABLE_TYPE',
                    ''
                ),

                if(
                    ifNull(member_casual, '') not in (
                        'member',
                        'casual'
                    ),
                    'INVALID_MEMBERSHIP',
                    ''
                ),

                if(
                    (
                        notEmpty(trimBoth(raw_start_lat))
                        and (
                            isNull(start_lat)
                            or abs(ifNull(start_lat, 0.0)) > 90
                            or not isFinite(ifNull(start_lat, 0.0))
                        )
                    )
                    or
                    (
                        notEmpty(trimBoth(raw_start_lng))
                        and (
                            isNull(start_lng)
                            or abs(ifNull(start_lng, 0.0)) > 180
                            or not isFinite(ifNull(start_lng, 0.0))
                        )
                    )
                    or
                    (
                        notEmpty(trimBoth(raw_end_lat))
                        and (
                            isNull(end_lat)
                            or abs(ifNull(end_lat, 0.0)) > 90
                            or not isFinite(ifNull(end_lat, 0.0))
                        )
                    )
                    or
                    (
                        notEmpty(trimBoth(raw_end_lng))
                        and (
                            isNull(end_lng)
                            or abs(ifNull(end_lng, 0.0)) > 180
                            or not isFinite(ifNull(end_lng, 0.0))
                        )
                    ),
                    'INVALID_COORDINATES',
                    ''
                )

            ]
        ) as error_codes,

        -- Warnings do not cause rejection.

        arrayFilter(
            code -> code != '',

            [
                if(
                    isNull(start_station_id)
                    or isNull(end_station_id),
                    'MISSING_STATION_ID',
                    ''
                ),

                if(
                    isNotNull(duration_seconds)
                    and (
                        duration_seconds < 60
                        or duration_seconds > 86400
                    ),
                    'UNUSUAL_DURATION',
                    ''
                )
            ]
        ) as warning_codes

    from source_data

)

-- STEP 5: Assign the final validation status.

select
    *,

    if(
        length(error_codes) > 0,
        'REJECTED',
        'VALID'
    ) as validation_status

from validated