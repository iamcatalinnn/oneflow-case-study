SELECT
    fct_doc.activity_date,
    fct_doc.account_id,
    dim_acc.company_name,
    dim_acc.plan,
    dim_acc.country,
    dim_acc.is_paying,
    fct_doc.document_signed_count
FROM
    {{ ref('fct_document_signing_daily') }}      AS fct_doc 
LEFT JOIN
    {{ ref('dim_accounts') }}                       AS dim_acc
    ON fct_doc.account_id = dim_acc.account_id