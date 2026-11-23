from __future__ import annotations

import os
import sys
import uuid

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
    "connect_timeout": int(env("DB_TIMEOUT", "10")),
}

SQL_LOG = "INSERT INTO etl.etl_log(run_id, step_name, status, message) VALUES (%s,%s,%s,%s);"

SQL_BUILD_STAGING = r'''
DROP TABLE IF EXISTS etl.bank_staging_clean;

CREATE TABLE etl.bank_staging_clean (
  record_id      SERIAL PRIMARY KEY,
  age            INT NOT NULL CHECK (age BETWEEN 18 AND 100),
  job            TEXT NOT NULL,
  marital        TEXT NOT NULL CHECK (marital IN ('married','single','divorced')),
  education      TEXT NOT NULL,
  default_flag   TEXT NOT NULL CHECK (default_flag IN ('yes','no')),
  balance        INT NOT NULL,
  housing        TEXT NOT NULL CHECK (housing IN ('yes','no')),
  loan           TEXT NOT NULL CHECK (loan IN ('yes','no')),
  contact        TEXT NOT NULL,
  day            INT NOT NULL CHECK (day BETWEEN 1 AND 31),
  month          TEXT NOT NULL CHECK (month IN ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec')),
  duration       INT NOT NULL CHECK (duration >= 0),
  campaign       INT NOT NULL CHECK (campaign >= 1),
  pdays          INT NOT NULL CHECK (pdays >= -1),
  previous       INT NOT NULL CHECK (previous >= 0),
  poutcome       TEXT NOT NULL,
  deposit        TEXT NOT NULL CHECK (deposit IN ('yes','no'))
);

WITH parsed AS (
  SELECT
    raw_id,
    CASE
      WHEN age_text ~ '^[-+]?[0-9]+$' THEN age_text::INT
      ELSE NULL
    END AS age,
    LOWER(TRIM(job)) AS job,
    LOWER(TRIM(marital)) AS marital,
    LOWER(TRIM(education)) AS education,
    LOWER(TRIM(default_text)) AS default_flag,
    CASE
      WHEN balance_text ~ '^[-+]?[0-9]+$' THEN balance_text::INT
      ELSE NULL
    END AS balance,
    LOWER(TRIM(housing)) AS housing,
    LOWER(TRIM(loan)) AS loan,
    LOWER(TRIM(contact)) AS contact,
    CASE
      WHEN day_text ~ '^[0-9]+$' THEN day_text::INT
      ELSE NULL
    END AS day,
    LOWER(TRIM(month)) AS month,
    CASE
      WHEN duration_text ~ '^[0-9]+$' THEN duration_text::INT
      ELSE NULL
    END AS duration,
    CASE
      WHEN campaign_text ~ '^[0-9]+$' THEN campaign_text::INT
      ELSE NULL
    END AS campaign,
    CASE
      WHEN pdays_text ~ '^[-+]?[0-9]+$' THEN pdays_text::INT
      ELSE NULL
    END AS pdays,
    CASE
      WHEN previous_text ~ '^[0-9]+$' THEN previous_text::INT
      ELSE NULL
    END AS previous,
    LOWER(TRIM(poutcome)) AS poutcome,
    LOWER(TRIM(deposit)) AS deposit
  FROM raw.bank_raw
),
cleaned AS (
  SELECT
    age, job, marital, education, default_flag,
    balance, housing, loan, contact, day,
    month, duration, campaign, pdays,
    previous, poutcome, deposit
  FROM parsed
  WHERE age IS NOT NULL AND age BETWEEN 18 AND 100
    AND balance IS NOT NULL
    AND day IS NOT NULL AND day BETWEEN 1 AND 31
    AND duration IS NOT NULL AND duration >= 0
    AND campaign IS NOT NULL AND campaign >= 1
    AND pdays IS NOT NULL AND pdays >= -1
    AND previous IS NOT NULL AND previous >= 0
    AND NULLIF(job, '') IS NOT NULL
    AND NULLIF(marital, '') IS NOT NULL AND marital IN ('married','single','divorced')
    AND NULLIF(education, '') IS NOT NULL
    AND NULLIF(default_flag, '') IS NOT NULL AND default_flag IN ('yes','no')
    AND NULLIF(housing, '') IS NOT NULL AND housing IN ('yes','no')
    AND NULLIF(loan, '') IS NOT NULL AND loan IN ('yes','no')
    AND NULLIF(contact, '') IS NOT NULL
    AND NULLIF(month, '') IS NOT NULL AND month IN ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec')
    AND NULLIF(poutcome, '') IS NOT NULL
    AND NULLIF(deposit, '') IS NOT NULL AND deposit IN ('yes','no')
)
INSERT INTO etl.bank_staging_clean
  (age, job, marital, education, default_flag, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome, deposit)
SELECT age, job, marital, education, default_flag, balance, housing, loan, contact, day, month, duration, campaign, pdays, previous, poutcome, deposit
FROM cleaned;
'''

SQL_LOAD_DW = '''
TRUNCATE TABLE dw.bank_fact RESTART IDENTITY;

INSERT INTO dw.bank_fact(
  age, job, marital, education, default_flag,
  balance, housing, loan, contact, day,
  month, duration, campaign, pdays,
  previous, poutcome, deposit
)
SELECT
  age, job, marital, education, default_flag,
  balance, housing, loan, contact, day,
  month, duration, campaign, pdays,
  previous, poutcome, deposit
FROM etl.bank_staging_clean;
'''

SQL_VALIDATE_DUP = """
SELECT age, job, marital, education, default_flag, balance, housing, loan,
       contact, day, month, duration, campaign, pdays, previous, poutcome, deposit,
       COUNT(*) AS cnt
FROM dw.bank_fact
GROUP BY age, job, marital, education, default_flag, balance, housing, loan,
         contact, day, month, duration, campaign, pdays, previous, poutcome, deposit
HAVING COUNT(*) > 1
LIMIT 10;
"""
SQL_COUNT = "SELECT (SELECT COUNT(*) FROM etl.bank_staging_clean), (SELECT COUNT(*) FROM dw.bank_fact);"

def log(cur, step: str, status: str, msg: str = ""):
    cur.execute(SQL_LOG, (RUN_ID, step, status, msg))

def main():
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            log(cur,"PIPELINE","START","Transform + Load bank data")
            log(cur,"TRANSFORM","START","Build staging")
            cur.execute(SQL_BUILD_STAGING)
            log(cur,"TRANSFORM","SUCCESS","Staging built")

            log(cur,"LOAD","START","Load to DW (truncate + insert)")
            cur.execute(SQL_LOAD_DW)
            log(cur,"LOAD","SUCCESS","Load done")

            log(cur,"VALIDATE","START","Check duplicates + counts")
            cur.execute(SQL_VALIDATE_DUP)
            dups = cur.fetchall()
            if dups:
                dup_info = "; ".join(str(r) for r in dups[:3])
                raise RuntimeError(f"Duplicate rows in dw.bank_fact (by all business columns): {dup_info}")

            cur.execute(SQL_COUNT)
            staging_cnt, dw_cnt = cur.fetchone()
            if staging_cnt <= 0:
                raise RuntimeError("staging_cnt=0 (no valid rows after cleaning)")
            if staging_cnt != dw_cnt:
                raise RuntimeError(f"Row count mismatch: staging={staging_cnt}, dw={dw_cnt}")
            log(cur,"VALIDATE","SUCCESS",f"staging_cnt={staging_cnt}, dw_cnt={dw_cnt}")

            log(cur,"PIPELINE","SUCCESS","ETL finished")
        conn.commit()
        print(f"ETL PASSED | RUN_ID={RUN_ID} | staging={staging_cnt} | dw={dw_cnt}")
        sys.exit(0)
    except Exception as e:
        conn.rollback()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                log(cur,"PIPELINE","FAIL",str(e))
        except Exception:
            pass
        print(f"ETL FAILED | RUN_ID={RUN_ID} | ERROR={e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
