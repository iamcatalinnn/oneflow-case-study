{% docs stg_accounts_model %}

Standardized account staging model.

Purpose:
- clean account attributes from the raw layer
- normalize plan and country values
- provide a trustworthy account base for downstream joins and marts

This model is intentionally lightweight because the source account data is relatively clean.

{% enddocs %}

{% docs stg_events_model %}

Standardized event staging model for analytics.

Purpose:
- remove direct personal data from the analytics event path
- normalize event attributes
- expose data quality issues explicitly
- rank duplicate event IDs without hiding source problems
- provide an `is_valid_for_analytics` flag for downstream filtering

This model preserves all source rows while making data quality issues transparent.

Key design choices:
- duplicate events are not dropped silently
- invalid timestamps are flagged, not replaced with fake values
- missing business-critical fields remain visible as quality issues
- downstream intermediate models decide which rows are eligible for analytics

{% enddocs %}

{% docs stg_user_identity_model %}

Restricted identity staging model containing direct personal data separated from analytics events.

Purpose:
- isolate direct identifiers such as name, email, phone
- support governance and controlled access patterns
- keep marts and analytics-ready event models free of unnecessary personal data

This model is not intended for direct BI reporting.

{% enddocs %}

{% docs stg_user_ip_addresses_model %}

Restricted identity staging model containing user IP address history.

This model captures the relationship between users and the IP addresses observed in event data.
Unlike user identity attributes (such as name or email), IP addresses are not stable at the user level and may change across sessions, devices, or networks.

To avoid misleading assumptions, IP addresses are modeled separately from `stg_user_identity` at a user-IP grain.

Typical use cases:
- security and anomaly detection
- auditing and traceability
- understanding user access patterns

This model is not intended for direct BI reporting and is excluded from the main analytics layer used to answer business questions.

{% enddocs %}
