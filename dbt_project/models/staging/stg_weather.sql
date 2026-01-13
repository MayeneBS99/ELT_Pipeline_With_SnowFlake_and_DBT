-- weather data cleaning

{{config(materialized = 'view')}}

SELECT 
    "name"                                             AS city,

    "weather"[0]:main::string                          AS weather_main,
    "weather"[0]:description::string                   AS weather_description,

    to_timestamp("dt")                                 AS weather_ts_utc,	
    to_timestamp("dt" + "timezone")                    AS weather_ts_local,

    "coord.lon"::float                                 AS longitude,	
    "coord.lat"::float                                 AS latitude,

    "main.temp",	
    "main.feels_like",
    "main.temp_min",
    "main.temp_max",
    "main.pressure",	
    "main.humidity",
    "wind.speed",

    "snapshot_ts"

FROM {{source('RAW', 'RAW_WEATHER')}}