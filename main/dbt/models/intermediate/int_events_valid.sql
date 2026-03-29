SELECT
    event_id,
    user_id,
    account_id,
    event_type,
    event_ts,
    user_country,
    user_agent,
    ingested_at
FROM {{ ref('stg_events') }}
WHERE 
    event_id_rank = 1
    AND is_valid_for_analytics = TRUE
