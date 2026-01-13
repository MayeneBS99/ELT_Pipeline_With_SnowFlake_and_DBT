-- Velib information data cleaning

SELECT 
    "station_id"::string AS station_id, 
    "stationCode"::string AS stationCode,
    "name",

    "lat"::float AS latitude,
    "lon"::float AS longitude,

    "capacity",
    "snapshot_ts"

FROM {{source('RAW', 'VELIB_STATION_INFORMATION_RAW')}}