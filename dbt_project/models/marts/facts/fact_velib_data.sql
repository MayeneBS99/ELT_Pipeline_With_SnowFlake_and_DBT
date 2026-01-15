-- fact table about velib and weather

{{config(
    materialized = "incremental",
    unique_key="snapshot_ts_station_id"
    
    )}}

WITH base AS (

    SELECT 
        i.STATION_ID,
        i."snapshot_ts",
        i.STATIONCODE, 
        i."num_bikes_available"  ,

        i.NUM_MECHANICAL_BIKES_AVAILABLE,
        i.NUM_EBIKE_BIKES_AVAILABLE,
        
        i."num_docks_available", 
        i."is_installed", 
        i."is_returning", 
        i."is_renting",

        i.STATUS_TS_UTC, 

        -- columns about weather
        j.WEATHER_TS_LOCAL,
        j."main.temp",	
        j."main.feels_like",
        j."main.humidity",
        j."wind.speed",

        -- columns from velib dimension
        
        k."capacity"

    FROM {{ref('stg_velib_status')}} AS i
    LEFT JOIN {{ref('stg_weather')}} AS j on i."snapshot_ts" = j."snapshot_ts"
    LEFT JOIN {{ref('stg_velib_informations')}} AS k on i.STATION_ID = k.STATION_ID
),

metrics AS (

    SELECT 
        *,
        -- occupency rates
        1 - ("num_bikes_available"/ NULLIF("capacity", 0)) AS occupency_rate_theorical,
        1 - ("num_bikes_available" / NULLIF("num_docks_available", 0)) AS occupancy_rate_operational,

        -- availability rate
        NUM_MECHANICAL_BIKES_AVAILABLE / NULLIF("num_bikes_available", 0) AS mechanical_bike_rate,
        NUM_EBIKE_BIKES_AVAILABLE / NULLIF("num_bikes_available", 0) AS ebike_rate,

        -- out_of_service_flag
        CASE WHEN "num_bikes_available" = 0 THEN 1
        ELSE 0
        END AS out_of_service_flag,

        -- delta flag
        "num_bikes_available" -
            LAG("num_bikes_available") OVER (
                PARTITION BY STATION_ID 
                ORDER BY "snapshot_ts"
            ) AS delta_bikes,

        -- time_diff
        
        DATEDIFF(
            'minute',
                LAG("snapshot_ts") OVER (
                    PARTITION BY STATION_ID
                    ORDER BY "snapshot_ts"
                ),
            "snapshot_ts"
            ) AS time_diff_minutes

        DATEDIFF(
            'hour',
                LAG("snapshot_ts") OVER (
                    PARTITION BY STATION_ID
                    ORDER BY "snapshot_ts"
                ),
            "snapshot_ts"
            ) AS time_diff_hours


    FROM base
            
)

SELECT 
    *,

    -- rotation_rate
    ABS(delta_bikes)/NULLIF("capacity", 0) AS rotation_rate,

    -- Moving Average
    AVG(occupancy_rate_operational) OVER (
        PARTITION BY STATION_ID
        ORDER BY "snapshot_ts"
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS moving_avg_occupency_3h,

    -- primary key
    STATION_ID || '_' || "snapshot_ts" AS snapshot_ts_station_id

FROM metrics    
