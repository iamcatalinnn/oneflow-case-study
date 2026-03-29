SELECT
	CAST(account_id AS INT) AS account_id,
	TRIM(company_name) AS company_name,
	LOWER(TRIM(plan)) AS plan,
	UPPER(TRIM(country)) AS country,
	CAST(created_date AS DATE) AS created_date,
	CAST(is_paying AS BOOLEAN) AS is_paying,
	ingested_at
FROM
	{{ source('raw', 'accounts') }}