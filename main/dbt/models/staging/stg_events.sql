WITH base AS (
SELECT
	CAST(event_id AS INT) AS event_id,
	CAST(user_id AS INT) AS user_id,
	CAST(account_id AS INT) AS account_id,
	LOWER(TRIM(event_type)) AS event_type,
	CAST(event_ts AS TIMESTAMP) AS event_ts,
	UPPER(NULLIF(TRIM(user_country), '')) AS user_country,
	NULLIF(TRIM(user_agent), '') AS user_agent,
	ingested_at
FROM
	{{ source('raw', 'events') }}
),

flagged AS (
SELECT
	event_id,
	user_id,
	account_id,
	event_type,
	event_ts,
	user_country,
	user_agent,
	ingested_at,
	CASE
		WHEN event_type IS NULL THEN TRUE
		ELSE FALSE
	END                                                                 AS is_missing_event_type,
	CASE
		WHEN account_id IS NULL THEN TRUE
		ELSE FALSE
	END                                                                 AS is_missing_account_id,
	CASE
		WHEN user_country IS NULL THEN TRUE
		ELSE FALSE
	END                                                                 AS is_missing_user_country,
	CASE
		WHEN event_ts IS NULL THEN TRUE
		WHEN event_ts < CAST('2000-01-01' AS timestamp) THEN TRUE
		WHEN event_ts > current_timestamp THEN TRUE
		ELSE FALSE
	END                                                                 AS is_invalid_event_ts
FROM
	base
),

ranked AS (
SELECT
	*,
	ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY
    event_type, event_ts DESC
    )                                                                       AS event_id_rank,
	count(*) OVER (
        PARTITION BY event_id
    )                                                                       AS event_id_duplicate_count
FROM
	flagged
)

SELECT
	event_id,
	user_id,
	account_id,
	event_type,
	event_ts,
	user_country,
	user_agent,
	ingested_at,
	is_missing_event_type,
	is_missing_account_id,
	is_missing_user_country,
	is_invalid_event_ts,
	CASE
		WHEN event_id_duplicate_count > 1 THEN TRUE
		ELSE FALSE
    END                                                                     AS is_duplicate_event_id,
	event_id_rank,
	CASE
		WHEN event_id_rank = 1
		AND is_missing_account_id = FALSE
		AND is_missing_event_type = FALSE
		AND is_invalid_event_ts = FALSE
        THEN TRUE
		ELSE FALSE
	END                                                                     AS is_valid_for_analytics
FROM
	ranked