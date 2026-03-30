WITH base AS (
SELECT
	CAST(user_id AS INT) AS user_id,
	user_name,
	user_email,
	user_phone,
	ingested_at,
	ROW_NUMBER() OVER (
        PARTITION BY 
            user_id
        ORDER BY
	        ingested_at DESC
    ) AS rn
FROM
	{{ source('raw', 'events') }}
WHERE
	user_id IS NOT NULL
),

final AS (SELECT
	user_id,
	user_name,
	user_email,
	user_phone,
	ingested_at,
    CASE
        WHEN user_name IS NULL THEN TRUE
        ELSE FALSE
    END AS is_missing_user_name,
    CASE
        WHEN user_email IS NULL THEN TRUE
        ELSE FALSE
    END AS is_missing_user_email,
    CASE
        WHEN user_phone IS NULL THEN TRUE
        ELSE FALSE
    END AS is_missing_user_phone,
    CASE
        WHEN user_email like 'ghost%@unknown.com' THEN TRUE
        ELSE FALSE
    END AS is_ghost_email
FROM
	base
WHERE
	rn = 1
)

SELECT 
    *,
    CASE 
        WHEN is_missing_user_email = FALSE AND is_ghost_email = FALSE THEN TRUE
        ELSE FALSE
    END AS is_valid_identity_record
FROM 
    final

