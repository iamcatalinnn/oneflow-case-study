SELECT
    account_id,
    activity_date,
    COUNT(*) AS document_signed_count
FROM
    {{ ref('int_document_signed_events') }}
GROUP BY 
    account_id, 
    activity_date
