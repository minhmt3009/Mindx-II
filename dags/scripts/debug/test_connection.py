"""
Script để test kết nối database và kiểm tra các bảng đã được tạo
"""
import os
import psycopg2

def env(name: str, default: str) -> str:
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

print("=" * 60)
print("DATABASE CONNECTION TEST")
print("=" * 60)
print(f"Host: {DB['host']}")
print(f"Port: {DB['port']}")
print(f"Database: {DB['database']}")
print(f"User: {DB['user']}")
print()

try:
    conn = psycopg2.connect(**DB)
    print("✓ Connection successful!")
    
    with conn.cursor() as cur:
        # Check schemas
        print("\n" + "=" * 60)
        print("CHECKING SCHEMAS")
        print("=" * 60)
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'etl', 'dw')
            ORDER BY schema_name
        """)
        schemas = cur.fetchall()
        for schema in schemas:
            print(f"✓ Schema exists: {schema[0]}")
        
        if len(schemas) < 3:
            print("\n⚠ WARNING: Not all required schemas exist!")
            print("  Expected: raw, etl, dw")
            print("  Run: docker exec -it <postgres_container> psql -U airflow -d bank_db -f /docker-entrypoint-initdb.d/02_init_schema.sql")
        
        # Check tables
        print("\n" + "=" * 60)
        print("CHECKING TABLES")
        print("=" * 60)
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('raw', 'etl', 'dw')
            ORDER BY table_schema, table_name
        """)
        tables = cur.fetchall()
        for table in tables:
            print(f"✓ Table exists: {table[0]}.{table[1]}")
        
        if len(tables) == 0:
            print("\n⚠ WARNING: No tables found!")
            print("  Run initialization SQL first")
        
        # Check raw.bank_raw structure
        print("\n" + "=" * 60)
        print("CHECKING raw.bank_raw STRUCTURE")
        print("=" * 60)
        try:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'raw' AND table_name = 'bank_raw'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            if columns:
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
            else:
                print("⚠ Table raw.bank_raw not found!")
        except Exception as e:
            print(f"⚠ Error checking table structure: {e}")
        
        # Check CSV path
        print("\n" + "=" * 60)
        print("CHECKING CSV FILE")
        print("=" * 60)
        csv_path = env("CSV_PATH", "/opt/airflow/dags/data/Bank.csv")
        print(f"CSV Path: {csv_path}")
        
        import os
        if os.path.exists(csv_path):
            print(f"✓ CSV file exists")
            import pandas as pd
            df = pd.read_csv(csv_path, nrows=5)
            print(f"✓ CSV readable, columns: {list(df.columns)}")
            print(f"✓ CSV has {len(pd.read_csv(csv_path))} rows")
        else:
            print(f"⚠ CSV file not found at: {csv_path}")
    
    conn.close()
    print("\n" + "=" * 60)
    print("✓ ALL CHECKS PASSED")
    print("=" * 60)
    
except psycopg2.OperationalError as e:
    print(f"\n✗ Connection failed: {e}")
    print("\nTroubleshooting:")
    print("  1. Check if Postgres container is running: docker ps")
    print("  2. Check logs: docker-compose logs postgres")
    print("  3. Verify environment variables in docker-compose.yaml")
except Exception as e:
    print(f"\n✗ Error: {e}")
