import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=False)

DATA_DIR = BASE_DIR / "data"

EXPECTED_ACCOUNTS_COLS = {
    "account_id", "company_name", "plan", "country", "created_date", "is_paying"
}
EXPECTED_EVENTS_COLS = {
    "event_id", "user_id", "user_name", "user_email", "user_phone",
    "user_country", "account_id", "ip_address", "event_type", "timestamp", "user_agent"
}


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


DB_CONFIG = {
    "host": get_env("DB_HOST", "localhost"),
    "port": int(get_env("DB_PORT", "5433")),
    "dbname": get_env("DB_NAME"),
    "user": get_env("DB_USER"),
    "password": get_env("DB_PASSWORD"),
}


def print_db_config():
    safe_config = {**DB_CONFIG, "password": "***"}
    print(f"Using DB config: {safe_config}")


def na_to_none(value):
    if pd.isna(value):
        return None
    if isinstance(value, str) and value.lower() == "nan":
        return None
    return value


def validate_columns(df, expected, name):
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected

    if missing:
        raise ValueError(f"[{name}] Schema mismatch — missing columns: {sorted(missing)}")
    if extra:
        print(f"[{name}] WARNING: unexpected columns will be ignored: {sorted(extra)}")


def ensure_schemas(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS intermediate")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS marts")


def load_accounts(cursor, ingested_at):
    df = pd.read_csv(DATA_DIR / "accounts.csv")

    validate_columns(df, EXPECTED_ACCOUNTS_COLS, "accounts")

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce").dt.date

    rows = [
        (
            na_to_none(row["account_id"]),
            na_to_none(row["company_name"]),
            na_to_none(row["plan"]),
            na_to_none(row["country"]),
            na_to_none(row["created_date"]),
            na_to_none(row["is_paying"]),
            ingested_at,
        )
        for _, row in df.iterrows()
    ]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.accounts (
            account_id   INTEGER,
            company_name TEXT,
            plan         TEXT,
            country      TEXT,
            created_date DATE,
            is_paying    BOOLEAN,
            ingested_at  TIMESTAMP WITH TIME ZONE
        )
    """)
    cursor.execute("TRUNCATE TABLE raw.accounts")

    execute_values(
        cursor,
        """
        INSERT INTO raw.accounts
            (account_id, company_name, plan, country, created_date, is_paying, ingested_at)
        VALUES %s
        """,
        rows,
    )

    print(f"[accounts] Loaded {len(rows)} rows.")


def load_events(cursor, ingested_at):
    df = pd.read_csv(DATA_DIR / "events.csv", dtype={"user_phone": str})

    validate_columns(df, EXPECTED_EVENTS_COLS, "events")

    df["event_ts"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.drop(columns=["timestamp"])

    rows = [
        (
            na_to_none(row["event_id"]),
            na_to_none(row["user_id"]),
            na_to_none(row["user_name"]),
            na_to_none(row["user_email"]),
            na_to_none(row["user_phone"]),
            na_to_none(row["user_country"]),
            na_to_none(row["account_id"]),
            na_to_none(row["ip_address"]),
            na_to_none(row["event_type"]),
            na_to_none(row["event_ts"]),
            na_to_none(row["user_agent"]),
            ingested_at,
        )
        for _, row in df.iterrows()
    ]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.events (
            event_id     INTEGER,
            user_id      INTEGER,
            user_name    TEXT,
            user_email   TEXT,
            user_phone   TEXT,
            user_country TEXT,
            account_id   INTEGER,
            ip_address   TEXT,
            event_type   TEXT,
            event_ts     TIMESTAMP,
            user_agent   TEXT,
            ingested_at  TIMESTAMP WITH TIME ZONE
        )
    """)
    cursor.execute("TRUNCATE TABLE raw.events")

    execute_values(
        cursor,
        """
        INSERT INTO raw.events
            (event_id, user_id, user_name, user_email, user_phone, user_country,
             account_id, ip_address, event_type, event_ts, user_agent, ingested_at)
        VALUES %s
        """,
        rows,
    )

    print(f"[events] Loaded {len(rows)} rows.")


if __name__ == "__main__":
    ingested_at = datetime.now(timezone.utc)

    try:
        print_db_config()

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                ensure_schemas(cursor)
                load_accounts(cursor, ingested_at)
                load_events(cursor, ingested_at)

        print("Ingestion complete.")

    except ValueError as e:
        print(f"Validation error: {e}")
        sys.exit(1)

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)