from __future__ import annotations

import os
import psycopg2

def env(name: str, default: str):
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return v.strip()

DB = {
    "host": env("DB_HOST", "postgres"),
    "port": int(env("DB_PORT", "5432")),
    "database": env("DB_NAME", "bank_db"),
    "user": env("DB_USER", "airflow"),
    "password": env("DB_PASS", "airflow"),
}

conn=psycopg2.connect(**DB)
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM raw.bank_raw;")
    raw_cnt=cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.bank_fact;")
    dw_cnt=cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.bank_fact WHERE deposit='yes';")
    deposit_yes=cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.bank_fact WHERE deposit='no';")
    deposit_no=cur.fetchone()[0]
conn.close()
print(f"CHECK OK | raw_cnt={raw_cnt} | dw_cnt={dw_cnt} | deposit_yes={deposit_yes} | deposit_no={deposit_no}")
