"""
Airflow Configuration Setup
===========================

This file contains the necessary Airflow Variables and Connections
required for the Retail Sales ETL Pipeline.

Setup Instructions:
------------------
1. Set up Airflow Variables via UI (Admin -> Variables) or CLI
2. Configure Connections via UI (Admin -> Connections) or CLI
3. Ensure all credentials are stored in environment variables or secrets manager
"""

# ============================================================================
# AIRFLOW VARIABLES
# ============================================================================

AIRFLOW_VARIABLES = {
    # Email configuration
    "ALERT_EMAIL_LIST": "data-engineering@company.com,alerts@company.com",
    
    # S3 Configuration
    "S3_STAGING_BUCKET": "data-pipeline-staging",
    "S3_REGION": "us-east-1",
    
    # Snowflake Configuration
    "SNOWFLAKE_WAREHOUSE": "ETL_WH",
    "SNOWFLAKE_DATABASE": "ANALYTICS_DB",
    "SNOWFLAKE_SCHEMA": "SALES",
    "SNOWFLAKE_ROLE": "ETL_ROLE",
    
    # Data Quality Thresholds
    "MIN_RECORD_COUNT": "100",
    "MAX_NULL_PERCENTAGE": "5.0",
    
    # Retry Configuration
    "MAX_RETRIES": "3",
    "RETRY_DELAY_MINUTES": "5",
}

# ============================================================================
# AIRFLOW CONNECTIONS
# ============================================================================

# Connection IDs and their configuration
# Set these up via Airflow UI: Admin -> Connections

AIRFLOW_CONNECTIONS = {
    # PostgreSQL Regional Databases
    "postgres_us_east": {
        "conn_type": "postgres",
        "host": "us-east-db.company.com",
        "schema": "sales_db",
        "login": "${POSTGRES_US_EAST_USER}",  # Use env variable
        "password": "${POSTGRES_US_EAST_PASSWORD}",
        "port": 5432,
        "extra": {
            "connect_timeout": 30,
            "keepalives": 1,
            "keepalives_idle": 30
        }
    },
    
    "postgres_us_west": {
        "conn_type": "postgres",
        "host": "us-west-db.company.com",
        "schema": "sales_db",
        "login": "${POSTGRES_US_WEST_USER}",
        "password": "${POSTGRES_US_WEST_PASSWORD}",
        "port": 5432,
    },
    
    "postgres_europe": {
        "conn_type": "postgres",
        "host": "eu-db.company.com",
        "schema": "sales_db",
        "login": "${POSTGRES_EUROPE_USER}",
        "password": "${POSTGRES_EUROPE_PASSWORD}",
        "port": 5432,
    },
    
    "postgres_asia_pacific": {
        "conn_type": "postgres",
        "host": "apac-db.company.com",
        "schema": "sales_db",
        "login": "${POSTGRES_APAC_USER}",
        "password": "${POSTGRES_APAC_PASSWORD}",
        "port": 5432,
    },
    
    # Default PostgreSQL (for staging)
    "postgres_default": {
        "conn_type": "postgres",
        "host": "staging-db.company.com",
        "schema": "staging",
        "login": "${POSTGRES_STAGING_USER}",
        "password": "${POSTGRES_STAGING_PASSWORD}",
        "port": 5432,
    },
    
    # Snowflake Connection
    "snowflake_default": {
        "conn_type": "snowflake",
        "account": "company.us-east-1",
        "warehouse": "ETL_WH",
        "database": "ANALYTICS_DB",
        "schema": "SALES",
        "login": "${SNOWFLAKE_USER}",
        "password": "${SNOWFLAKE_PASSWORD}",
        "role": "ETL_ROLE",
        "extra": {
            "region": "us-east-1",
            "insecure_mode": False,
        }
    },
    
    # AWS Connection (for S3)
    "aws_default": {
        "conn_type": "aws",
        "login": "${AWS_ACCESS_KEY_ID}",
        "password": "${AWS_SECRET_ACCESS_KEY}",
        "extra": {
            "region_name": "us-east-1",
        }
    },
}

# ============================================================================
# CLI COMMANDS TO SET UP CONNECTIONS AND VARIABLES
# ============================================================================

CLI_SETUP_COMMANDS = """
# Set Airflow Variables
airflow variables set ALERT_EMAIL_LIST "data-engineering@company.com,alerts@company.com"
airflow variables set S3_STAGING_BUCKET "data-pipeline-staging"
airflow variables set S3_REGION "us-east-1"
airflow variables set SNOWFLAKE_WAREHOUSE "ETL_WH"
airflow variables set SNOWFLAKE_DATABASE "ANALYTICS_DB"
airflow variables set SNOWFLAKE_SCHEMA "SALES"
airflow variables set MIN_RECORD_COUNT "100"
airflow variables set MAX_NULL_PERCENTAGE "5.0"

# Set PostgreSQL Connections (US East)
airflow connections add postgres_us_east \\
    --conn-type postgres \\
    --conn-host us-east-db.company.com \\
    --conn-schema sales_db \\
    --conn-login $POSTGRES_US_EAST_USER \\
    --conn-password $POSTGRES_US_EAST_PASSWORD \\
    --conn-port 5432

# Set PostgreSQL Connections (US West)
airflow connections add postgres_us_west \\
    --conn-type postgres \\
    --conn-host us-west-db.company.com \\
    --conn-schema sales_db \\
    --conn-login $POSTGRES_US_WEST_USER \\
    --conn-password $POSTGRES_US_WEST_PASSWORD \\
    --conn-port 5432

# Set PostgreSQL Connections (Europe)
airflow connections add postgres_europe \\
    --conn-type postgres \\
    --conn-host eu-db.company.com \\
    --conn-schema sales_db \\
    --conn-login $POSTGRES_EUROPE_USER \\
    --conn-password $POSTGRES_EUROPE_PASSWORD \\
    --conn-port 5432

# Set PostgreSQL Connections (Asia Pacific)
airflow connections add postgres_asia_pacific \\
    --conn-type postgres \\
    --conn-host apac-db.company.com \\
    --conn-schema sales_db \\
    --conn-login $POSTGRES_APAC_USER \\
    --conn-password $POSTGRES_APAC_PASSWORD \\
    --conn-port 5432

# Set PostgreSQL Default (Staging)
airflow connections add postgres_default \\
    --conn-type postgres \\
    --conn-host staging-db.company.com \\
    --conn-schema staging \\
    --conn-login $POSTGRES_STAGING_USER \\
    --conn-password $POSTGRES_STAGING_PASSWORD \\
    --conn-port 5432

# Set Snowflake Connection
airflow connections add snowflake_default \\
    --conn-type snowflake \\
    --conn-login $SNOWFLAKE_USER \\
    --conn-password $SNOWFLAKE_PASSWORD \\
    --conn-schema SALES \\
    --conn-extra '{
        "account": "company.us-east-1",
        "warehouse": "ETL_WH",
        "database": "ANALYTICS_DB",
        "role": "ETL_ROLE",
        "region": "us-east-1"
    }'

# Set AWS Connection
airflow connections add aws_default \\
    --conn-type aws \\
    --conn-login $AWS_ACCESS_KEY_ID \\
    --conn-password $AWS_SECRET_ACCESS_KEY \\
    --conn-extra '{
        "region_name": "us-east-1"
    }'
"""

# ============================================================================
# ENVIRONMENT VARIABLES CHECKLIST
# ============================================================================

REQUIRED_ENV_VARIABLES = [
    # PostgreSQL
    "POSTGRES_US_EAST_USER",
    "POSTGRES_US_EAST_PASSWORD",
    "POSTGRES_US_WEST_USER",
    "POSTGRES_US_WEST_PASSWORD",
    "POSTGRES_EUROPE_USER",
    "POSTGRES_EUROPE_PASSWORD",
    "POSTGRES_APAC_USER",
    "POSTGRES_APAC_PASSWORD",
    "POSTGRES_STAGING_USER",
    "POSTGRES_STAGING_PASSWORD",
    
    # Snowflake
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    
    # AWS
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]

# ============================================================================
# SNOWFLAKE SETUP SQL
# ============================================================================

SNOWFLAKE_SETUP_SQL = """
-- Create warehouse for ETL workloads
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL operations';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
USE DATABASE ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS SALES;
USE SCHEMA SALES;

-- Create role for ETL processes
CREATE ROLE IF NOT EXISTS ETL_ROLE;

-- Grant privileges
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ETL_ROLE;
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE ETL_ROLE;
GRANT ALL ON SCHEMA SALES TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA SALES TO ROLE ETL_ROLE;

-- Create internal stage for data loading
CREATE STAGE IF NOT EXISTS sales_data_stage
    URL = 's3://data-pipeline-staging/'
    CREDENTIALS = (AWS_KEY_ID = '<access_key>' AWS_SECRET_KEY = '<secret_key>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0);

-- Create staging and final tables (these are also created by DAG)
CREATE TABLE IF NOT EXISTS staging.sales_data_stage (
    sale_id BIGINT,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    sale_date TIMESTAMP,
    quantity INTEGER,
    unit_price NUMBER(10,2),
    total_amount NUMBER(10,2),
    region VARCHAR(50),
    store_id VARCHAR(50),
    payment_method VARCHAR(50),
    discount_amount NUMBER(10,2),
    tax_amount NUMBER(10,2),
    net_amount NUMBER(10,2),
    gross_profit NUMBER(10,2),
    sale_year INTEGER,
    sale_month INTEGER,
    sale_day INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS analytics.sales_fact (
    sale_id BIGINT PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    sale_date TIMESTAMP,
    quantity INTEGER,
    unit_price NUMBER(10,2),
    total_amount NUMBER(10,2),
    region VARCHAR(50),
    store_id VARCHAR(50),
    payment_method VARCHAR(50),
    discount_amount NUMBER(10,2),
    tax_amount NUMBER(10,2),
    net_amount NUMBER(10,2),
    gross_profit NUMBER(10,2),
    sale_year INTEGER,
    sale_month INTEGER,
    sale_day INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(20)
)
CLUSTER BY (sale_date, region);

-- Create monitoring view
CREATE OR REPLACE VIEW analytics.etl_monitoring AS
SELECT 
    etl_batch_id,
    COUNT(*) as record_count,
    MIN(sale_date) as min_sale_date,
    MAX(sale_date) as max_sale_date,
    SUM(total_amount) as total_sales,
    COUNT(DISTINCT region) as region_count,
    MAX(etl_loaded_at) as last_loaded_at
FROM analytics.sales_fact
GROUP BY etl_batch_id
ORDER BY etl_batch_id DESC;
"""

if __name__ == "__main__":
    print("=" * 80)
    print("AIRFLOW CONFIGURATION SETUP")
    print("=" * 80)
    print("\\n1. Set up environment variables:")
    print("-" * 80)
    for var in REQUIRED_ENV_VARIABLES:
        print(f"export {var}=<value>")
    
    print("\\n2. Run CLI commands:")
    print("-" * 80)
    print(CLI_SETUP_COMMANDS)
    
    print("\\n3. Execute Snowflake setup:")
    print("-" * 80)
    print(SNOWFLAKE_SETUP_SQL)
    print("\\n" + "=" * 80)
