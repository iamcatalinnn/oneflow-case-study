{% docs int_events_valid_model %}

Trusted analytics event base.

This model applies the quality and deduplication rules prepared in `stg_events` and keeps only rows that are eligible for downstream analytics.

Rules enforced here:
- keep only the preferred duplicate row for each `event_id`
- require a non-null `account_id`
- require a non-null `event_type`
- require a valid timestamp

This model exists so downstream business models do not have to repeat generic event-quality logic.

{% enddocs %}

{% docs int_document_signed_events_model %}

Business-specific subset of valid product events containing only document signing activity.

This model filters `int_events_valid` to `event_type = 'document_signed'` and derives `activity_date` from the event timestamp.

It is the direct event-level input to the final analytics mart that answers the business question:
“How is document signing activity trending per account (and per plan/country)?”

{% enddocs %}
