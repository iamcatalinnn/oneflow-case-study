{% docs raw_source_overview %}

Raw landing layer for the case study.

These tables are loaded directly from the provided CSV files into Postgres with minimal transformation.
The purpose of this layer is to preserve source fidelity and provide a reproducible ingestion boundary.

The raw layer may still contain:
- duplicate event IDs
- missing values
- implausible timestamps
- direct personal data

These issues are intentionally handled downstream in staging and intermediate models rather than being hidden in ingestion.

{% enddocs %}

{% docs raw_events_source %}

Product usage events exported from the application.

Each event row contains product activity emitted by the platform, together with user information as it existed at the time of the event.
This source is the primary input for building document-signing analytics.

Because it reflects source truth, it may contain:
- duplicate events
- null values
- invalid timestamps
- personal data such as name, email, phone number, and IP address

During ingestion, the source `timestamp` field is loaded into Postgres as `event_ts` to avoid using a reserved SQL keyword.

{% enddocs %}

{% docs raw_accounts_source %}

Account and company data exported from the billing system.

This source provides the account attributes used to segment document-signing activity by account, plan, and country.
Compared with the event source, it is relatively clean and stable.

{% enddocs %}