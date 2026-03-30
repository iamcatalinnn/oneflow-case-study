#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Postgres..."
until nc -z postgres 5432; do
  sleep 1
done

echo "Running ingestion..."
python /app/ingestion/load_raw_data.py

echo "Running dbt models..."
dbt run --project-dir /app/main/dbt

echo "Running dbt tests..."
dbt test --project-dir /app/main/dbt

echo "Generating dbt docs..."
dbt docs generate --project-dir /app/main/dbt

echo "Pipeline completed successfully."