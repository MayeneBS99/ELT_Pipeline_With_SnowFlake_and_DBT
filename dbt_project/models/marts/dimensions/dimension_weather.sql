{{config(materialized = "table")}}

SELECT

    DISTINCT "snapshot_ts", -- as foreign key for fact_velib data
    WEATHER_TS_LOCAL, 
    WEATHER_MAIN,
    WEATHER_DESCRIPTION,
    "main.temp" AS temperature,

    -- temperature bucket
    CASE
        WHEN temperature < 5 THEN 'Very cold'
        WHEN temperature BETWEEN 5 AND 15 THEN 'Cold'
        WHEN temperature BETWEEN 15 AND 25 THEN 'Mild'
        ELSE 'Hot'
    END AS temperature_bucket

FROM {{ref('stg_weather')}}