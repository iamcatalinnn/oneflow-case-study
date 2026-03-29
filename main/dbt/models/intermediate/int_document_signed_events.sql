SELECT
    event_id,
    user_id,
    account_id,
    event_ts,
    CAST(event_ts AS DATE) AS activity_date,
    ingested_at
FROM 
    {{ ref('int_events_valid') }}
WHERE 
    event_type = 'document_signed'
