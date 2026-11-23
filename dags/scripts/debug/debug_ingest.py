"""
Debug script - chạy từng bước của ingest để tìm lỗi
"""
from __future__ import annotations
import os
import sys


def main():
    print("=" * 70)
    print("DEBUG: INGEST CSV TO RAW")
    print("=" * 70)

    # Step 1: Check environment variables
    print("\n[1] Checking environment variables...")
    env_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS", "CSV_PATH"]
    for var in env_vars:
        value = os.getenv(var)
        masked = "***" if var == "DB_PASS" and value else value
        print(f"  {var}: {masked}")

    # Step 2: Try import
    print("\n[2] Importing libraries...")
    try:
        import pandas as pd
        print("  ✓ pandas imported")
    except Exception as e:
        print(f"  ✗ pandas import failed: {e}")
        sys.exit(1)

    try:
        import psycopg2
        print("  ✓ psycopg2 imported")
    except Exception as e:
        print(f"  ✗ psycopg2 import failed: {e}")
        sys.exit(1)

    # Step 3: Test CSV reading
    print("\n[3] Testing CSV file...")
    csv_path = os.getenv("CSV_PATH", "/opt/airflow/dags/data/Bank.csv")
    print(f"  CSV Path: {csv_path}")

    try:
        if not os.path.exists(csv_path):
            print(f"  ✗ CSV file NOT found at: {csv_path}")
            print(f"  Checking directory...")
            dir_path = os.path.dirname(csv_path)
            if os.path.exists(dir_path):
                files = os.listdir(dir_path)
                print(f"  Files in {dir_path}: {files}")
            sys.exit(1)
        else:
            print(f"  ✓ CSV file exists")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        print(f"  ✓ CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        print(f"  Columns: {list(df.columns)}")

        # Check column mapping
        expected_cols = ["age","job","marital","education","default","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome","deposit"]
        missing = set(expected_cols) - set(df.columns)
        if missing:
            print(f"  ✗ Missing columns: {missing}")
            sys.exit(1)
        else:
            print(f"  ✓ All {len(expected_cols)} columns present")

    except SystemExit:
        raise
    except Exception as e:
        print(f"  ✗ CSV reading failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Step 4: Test database connection
    print("\n[4] Testing database connection...")
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

    print(f"  Connection params: {DB['user']}@{DB['host']}:{DB['port']}/{DB['database']}")

    try:
        conn = psycopg2.connect(**DB)
        print("  ✓ Database connection successful")

        # Check if table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'raw'
                    AND table_name = 'bank_raw'
                );
            """)
            exists = cur.fetchone()[0]
            if exists:
                print("  ✓ Table raw.bank_raw exists")

                # Check table structure
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'raw' AND table_name = 'bank_raw'
                    ORDER BY ordinal_position;
                """)
                columns = [row[0] for row in cur.fetchall()]
                print(f"  Table columns ({len(columns)}): {columns}")
            else:
                print("  ✗ Table raw.bank_raw does NOT exist!")
                print("  Run: docker exec -it <postgres_container> psql -U airflow -d bank_db -f /docker-entrypoint-initdb.d/02_init_schema.sql")
                sys.exit(1)

        conn.close()

    except SystemExit:
        raise
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 70)
    print("✓ ALL CHECKS PASSED - Ready to run ingest!")
    print("=" * 70)
    print("\nNow you can:")
    print("1. Trigger the DAG in Airflow UI")
    print("2. Or run: python /opt/airflow/dags/scripts/load_csv_to_postgres_raw.py")


if __name__ == "__main__":
    main()
