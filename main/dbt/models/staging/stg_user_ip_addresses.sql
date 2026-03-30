WITH base AS (
    SELECT
        CAST(user_id AS INTEGER) AS user_id,
        NULLIF(TRIM(ip_address), '') AS ip_address,
        CAST(event_ts AS TIMESTAMP) AS event_ts,
        ingested_at
    FROM {{ source('raw', 'events') }}
    WHERE 
        user_id IS NOT NULL
        AND NULLIF(TRIM(ip_address), '') IS NOT NULL

)

SELECT
    user_id,
    ip_address,
    MIN(event_ts) as first_seen_at,
    MAX(event_ts) as last_seen_at
FROM 
    base
GROUP BY 
    user_id, 
    ip_address
