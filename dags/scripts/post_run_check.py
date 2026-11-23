from __future__ import annotations

import os
import sys
import psycopg2

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return default
    return v.strip()

def main():
    DB = {
        "host": env("DB_HOST"),
        "port": int(env("DB_PORT")),
        "database": env("DB_NAME"),
        "user": env("DB_USER"),
        "password": env("DB_PASS"),
    }

    try:
        conn = psycopg2.connect(**DB)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.bank_raw;")
            raw_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dw.bank_fact;")
            dw_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dw.bank_fact WHERE deposit='yes';")
            deposit_yes = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dw.bank_fact WHERE deposit='no';")
            deposit_no = cur.fetchone()[0]
        conn.close()
        print(f"CHECK OK | raw_cnt={raw_cnt} | dw_cnt={dw_cnt} | deposit_yes={deposit_yes} | deposit_no={deposit_no}")
        if dw_cnt == 0:
            print("WARNING: dw.bank_fact is empty!")
            sys.exit(1)
        sys.exit(0)
    except Exception as e:
        print(f"CHECK FAILED | ERROR={e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
