SELECT
    account_id,
    company_name,
    plan,
    country,
    created_date,
    is_paying
FROM
    {{ ref('stg_accounts') }}
