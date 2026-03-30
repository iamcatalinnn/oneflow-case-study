{% docs dim_accounts_model %}

Trusted account dimension used to enrich document-signing activity with business attributes.

This model provides stable account-level segmentation fields such as:
- company name
- subscription plan
- country
- paying status
- account creation date

It is intentionally based on account-level attributes and serves as the reusable dimension for downstream analytics.

{% enddocs %}

{% docs fct_document_signing_daily_model %}

Fact table containing document signing activity aggregated by account and day.

Grain:
- one row per `account_id` and `activity_date`

Metric definition:
- `document_signed_count` = count of valid, deduplicated events where `event_type = 'document_signed'`

This model is the core analytical fact used to measure signing trends over time.

{% enddocs %}

{% docs mart_document_signing_activity_model %}

BI-ready mart for analyzing document signing activity by account, plan, and country.

This model joins the daily document-signing fact with trusted account attributes so that analysts and BI tools can answer the business question directly without additional joins.

It is designed to answer:
“How is document signing activity trending per account (and per plan/country)?”

The mart uses account country rather than event user country, because the business question is account-centric and account country is the more stable segmentation attribute.

{% enddocs %}