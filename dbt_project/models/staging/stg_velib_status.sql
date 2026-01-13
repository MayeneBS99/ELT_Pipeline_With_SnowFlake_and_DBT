-- Velib status information data cleaning

{{config(materialized = 'view')}}

SELECT 
    "station_id"::string AS station_id,
    "stationCode"::string AS stationCode, 
    "num_bikes_available" ,

    "num_bikes_available_types"[0]:mechanical::NUMBER  AS num_mechanical_bikes_available,
    "num_bikes_available_types"[1]:ebike::NUMBER  AS num_ebike_bikes_available,
    
    "num_docks_available", 
    "is_installed", 
    "is_returning", 
    "is_renting",

    to_timestamp("last_reported") AS status_ts_utc, 
    "snapshot_ts"

FROM {{source('RAW', 'VELIB_STATION_STATUS_RAW')}}