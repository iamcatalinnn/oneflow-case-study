# Oneflow Case Study – Data Pipeline

## Overview

This project implements a small, production-style data pipeline that ingests raw CSV data, applies data quality and governance rules, and produces analytics-ready tables to answer the business question:

> **How is document signing activity trending per account (and per plan/country)?**

The pipeline is built using:
- Python for ingestion
- PostgreSQL as the warehouse
- dbt for transformations and modeling
- Docker Compose for reproducible execution

---

## Project Structure

```
repo/
  data/              # provided CSV files
  ingestion/         # ingestion logic (Python)
  main/dbt/          # dbt project (models, tests, docs)
  containerization/  # Docker + pipeline runner
  README.md
```
---

---

## How to Run the Pipeline

### Option 1 – Recommended (Docker)

Run the entire pipeline with a single command:

docker compose -f containerization/docker-compose.yml up --build

This will:
1. Start a PostgreSQL instance
2. Load raw CSV data into `raw` schema
3. Run dbt models (staging → intermediate → marts)
4. Run dbt tests
5. Generate dbt documentation

---

### Option 2 – Local Execution

#### 1. Start Postgres

podman run -d \
  --name oneflow-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=oneflow \
  -p 5433:5432 \
  postgres:15

#### 2. Run ingestion

python ingestion/load_raw_data.py

#### 3. Run dbt

cd main/dbt

dbt run
dbt test
dbt docs generate

---

## Architecture Overview

The pipeline follows a layered data modeling approach:

### Raw Layer (`raw`)
- Direct ingestion from CSV files
- No transformations applied
- Includes ingestion timestamp (`ingested_at`)
- Preserves source data issues

---

### Staging Layer (`stg_*`)
- Standardizes and cleans raw data
- Handles:
  - null normalization
  - timestamp validation
  - type casting
  - duplicate detection (via ranking)
- Adds data quality flags instead of dropping data

Sensitive data is separated:
- `stg_user_identity` → user-level attributes
- `stg_user_ip_addresses` → user-IP relationship (correct grain)

---

### Intermediate Layer (`int_*`)
- Produces trusted, reusable datasets

Models:
- `int_events_valid`
- `int_document_signed_events`

---

### Mart Layer (`marts`)
- Analytics-ready models

Models:
- `dim_accounts`
- `fct_document_signing_daily`
- `mart_document_signing_activity`

Grain:
- account_id + activity_date

---

## Key Design Decisions & Trade-offs

- Layered architecture for clarity and maintainability
- Data quality issues are flagged, not hidden
- Sensitive data is isolated from analytics
- IP addresses modeled separately (correct grain)
- Account country used for segmentation (stable)
- No aggregations in staging layer

---

## Data Quality Issues Found

### Events Data
- Duplicate event_id
- Missing event_type
- Missing timestamps
- Invalid timestamps (1970 / future)
- Missing user_country
- Missing account_id

Handling:
- duplicates → ranking
- invalid timestamps → flags
- nulls → flags
- filtering → intermediate layer

---

### User Data
- inconsistent phone formats
- ghost users
- missing identity fields

---

### Accounts Data
- relatively clean

---

## Testing Strategy

- Staging → validate transformations
- Intermediate → enforce constraints
- Marts → ensure analytics reliability

---

## What I Would Do in Production

- Use proper ingestion pipelines (not CSV)
- Add orchestration (Airflow / Dagster)
- Implement monitoring & alerting
- Add access control for sensitive data
- Use incremental dbt models
- Add CI/CD pipelines

---

## Final Output

Main table:
mart_document_signing_activity

Example query:

SELECT
    activity_date,
    plan,
    country,
    SUM(document_signed_count) AS total_signed
FROM marts.mart_document_signing_activity
GROUP BY 
    activity_date, 
    plan, 
    country
ORDER BY 
    activity_date;

---

## Summary

This pipeline focuses on:
- correctness
- transparency
- maintainability
- production-oriented design
