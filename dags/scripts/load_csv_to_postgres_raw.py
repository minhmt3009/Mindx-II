from __future__ import annotations

import os
import sys
import uuid

import pandas as pd
import psycopg2

RUN_ID = f"RUN_{uuid.uuid4().hex[:8]}"

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return default
    return v.strip()

DB = {
    "host": env("DB_HOST"),
    "port": int(env("DB_PORT")),
    "database": env("DB_NAME"),
    "user": env("DB_USER"),
    "password": env("DB_PASS"),
}

CSV_PATH = env("CSV_PATH")

SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

def main() -> None:
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
        with conn.cursor() as cur:
            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "START", f"Loading CSV: {CSV_PATH} rows={len(df)}"))

            cur.execute("TRUNCATE TABLE raw.bank_raw RESTART IDENTITY;")
            insert_sql = '''
INSERT INTO raw.bank_raw(
  age_text, job, marital, education, default_text,
  balance_text, housing, loan, contact, day_text,
  month, duration_text, campaign_text, pdays_text,
  previous_text, poutcome, deposit
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
            rows = df[["age","job","marital","education","default","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome","deposit"]].values.tolist()
            cur.executemany(insert_sql, rows)

            cur.execute(SQL_LOG, (RUN_ID, "INGEST", "SUCCESS", f"Inserted rows={len(rows)}"))
        conn.commit()
        print(f"INGEST PASSED | RUN_ID={RUN_ID} | rows={len(df)}")
        sys.exit(0)
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SQL_LOG, (RUN_ID, "INGEST", "FAIL", str(e)))
        except Exception:
            pass
        print(f"INGEST FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
