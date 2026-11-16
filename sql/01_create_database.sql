-- Create bank_db database
-- This runs first (01_) when Postgres container initializes

-- Create database if not exists
CREATE DATABASE bank_db;

-- Grant privileges to airflow user
GRANT ALL PRIVILEGES ON DATABASE bank_db TO airflow;
