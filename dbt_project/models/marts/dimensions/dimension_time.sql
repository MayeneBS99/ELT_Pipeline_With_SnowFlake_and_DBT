{{config(materialized = "table")}}

SELECT
    DISTINCT "snapshot_ts", -- La clé primaire de ta dimension
    DATE("snapshot_ts") AS date_day,
    EXTRACT(YEAR FROM "snapshot_ts") AS year,
    EXTRACT(MONTH FROM "snapshot_ts") AS month,
    EXTRACT(HOUR FROM "snapshot_ts") AS hour,
    DAYNAME("snapshot_ts") AS day_name, 
    CASE 
        WHEN DAYNAME("snapshot_ts") IN ('Sat', 'Sun') THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM {{ref('stg_weather') }} 
