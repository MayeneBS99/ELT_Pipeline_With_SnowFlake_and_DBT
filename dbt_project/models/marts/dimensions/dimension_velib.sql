{{config(materialized = "table")}}

SELECT 
    DISTINCT STATION_ID, 
    STATIONCODE,
    "name",

    LATITUDE,
    LONGITUDE,
    "capacity"

    FROM {{ref('stg_velib_informations')}}