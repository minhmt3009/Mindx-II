
-- Connect to bank_db
\c bank_db

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS dw;


-- Raw table to store bank data from CSV (all text to handle any format)
DROP TABLE IF EXISTS raw.bank_raw CASCADE;
CREATE TABLE raw.bank_raw (
  raw_id           SERIAL PRIMARY KEY,
  age_text         TEXT,
  job              TEXT,
  marital          TEXT,
  education        TEXT,
  default_text     TEXT,
  balance_text     TEXT,
  housing          TEXT,
  loan             TEXT,
  contact          TEXT,
  day_text         TEXT,
  month            TEXT,
  duration_text    TEXT,
  campaign_text    TEXT,
  pdays_text       TEXT,
  previous_text    TEXT,
  poutcome         TEXT,
  deposit          TEXT,
  loaded_at        TIMESTAMP DEFAULT NOW()
);




-- ETL log table to track pipeline runs
DROP TABLE IF EXISTS etl.etl_log CASCADE;
CREATE TABLE etl.etl_log (
  log_id      SERIAL PRIMARY KEY,
  run_id      TEXT NOT NULL,
  step_name   TEXT NOT NULL,
  status      TEXT NOT NULL CHECK (status IN ('START','SUCCESS','FAIL')),
  message     TEXT,
  created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_etl_log_run_id ON etl.etl_log(run_id);
CREATE INDEX idx_etl_log_created_at ON etl.etl_log(created_at);

-- Staging table will be created dynamically by ETL script
-- (etl.bank_staging_clean)


-- Bank fact table (cleaned and validated data)
DROP TABLE IF EXISTS dw.bank_fact CASCADE;
CREATE TABLE dw.bank_fact (
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
  deposit        TEXT NOT NULL CHECK (deposit IN ('yes','no')),
  loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX idx_bank_fact_deposit ON dw.bank_fact(deposit);
CREATE INDEX idx_bank_fact_job ON dw.bank_fact(job);
CREATE INDEX idx_bank_fact_marital ON dw.bank_fact(marital);
CREATE INDEX idx_bank_fact_age ON dw.bank_fact(age);
CREATE INDEX idx_bank_fact_month ON dw.bank_fact(month);



-- Grant all privileges to airflow user
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA etl TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA dw TO airflow;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO airflow;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO airflow;



-- Check schemas
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'etl', 'dw');

-- Check tables
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_schema IN ('raw', 'etl', 'dw')
ORDER BY table_schema, table_name;
