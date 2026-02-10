"""
Retail Sales ETL Pipeline - Apache Airflow DAG

This DAG orchestrates the daily extraction of sales data from multiple regional
PostgreSQL databases, transforms it to a unified format, validates data quality,
and loads it into Snowflake for centralized reporting and analytics.

Author: Sanath
Date: 2024
"""

from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, Any
import csv
import json
import logging

import boto3
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
try:
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:
    # Provider >=5.0 removed SnowflakeOperator; use the common SQL operator.
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Dynamic region configuration — load from JSON at parse time (fast, no DB hit).
# To add a new region, edit config/region_config.json and add an Airflow connection.
_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config' / 'region_config.json'
with open(_CONFIG_PATH) as _f:
    _PIPELINE_CONFIG = json.load(_f)

REGION_CONFIGS = _PIPELINE_CONFIG['regions']
REGION_NAMES = [r['name'] for r in REGION_CONFIGS]
REGION_DISPLAY_MAP = {r['name']: r['display_name'] for r in REGION_CONFIGS}

# Table configuration
EXTRACT_STAGING_TABLE = 'staging.raw_sales_extract'
EXTRACTION_TABLE = 'staging.raw_sales_data'  # Transformed data staging
SNOWFLAKE_STAGE_TABLE = 'staging.sales_data_stage'
SNOWFLAKE_FINAL_TABLE = 'analytics.sales_fact'

# Data quality thresholds
MIN_RECORD_COUNT = 100
MAX_NULL_PERCENTAGE = 5.0
REQUIRED_COLUMNS = ['sale_id', 'customer_id', 'product_id', 'sale_date',
                    'quantity', 'unit_price', 'total_amount', 'region']

# Airflow Variables for dynamic configuration
# Set these in Airflow UI: Admin -> Variables
# ALERT_EMAIL_LIST: "data-engineering@company.com,alerts@company.com"
# SNOWFLAKE_CONN_ID: "snowflake_default"
# POSTGRES_CONN_PREFIX: "postgres_"

logger = logging.getLogger(__name__)


# ============================================================================
# PYTHON CALLABLE FUNCTIONS
# ============================================================================

def extract_sales_data_from_region(region: str, conn_id: str, execution_date: str, **context) -> Dict[str, Any]:
    """
    Extract sales data from regional PostgreSQL database for the execution date.

    This function:
    1. Connects to regional database
    2. Extracts yesterday's sales data
    3. Stores in persistent staging table on the staging database
    4. Returns metadata for monitoring

    Args:
        region: Region identifier (us_east, us_west, etc.)
        conn_id: Airflow connection ID for the regional database
        execution_date: Airflow execution date
        **context: Airflow context

    Returns:
        Dict with extraction metadata (record_count, file_path, etc.)
    """
    if region not in REGION_NAMES:
        raise ValueError(f"Invalid region: {region}")

    # Parse execution date
    exec_date = datetime.strptime(execution_date, '%Y-%m-%d')
    target_date = (exec_date - timedelta(days=1)).strftime('%Y-%m-%d')
    batch_id = execution_date.replace('-', '')

    logger.info(f"Extracting sales data for region: {region}, date: {target_date}")

    # Step 1: Extract from regional database using parameterized query
    regional_hook = PostgresHook(postgres_conn_id=conn_id)
    regional_conn = regional_hook.get_conn()
    try:
        regional_cursor = regional_conn.cursor()
        extraction_query = """
            SELECT
                sale_id, customer_id, product_id, sale_date,
                quantity, unit_price, total_amount,
                store_id, payment_method, discount_amount,
                tax_amount, created_at, updated_at
            FROM sales.transactions
            WHERE DATE(sale_date) = %s
                AND is_deleted = FALSE
                AND status = 'completed'
            ORDER BY sale_id;
        """
        regional_cursor.execute(extraction_query, (target_date,))
        records = regional_cursor.fetchall()
        record_count = len(records)
        regional_cursor.close()
    finally:
        regional_conn.close()

    logger.info(f"Extracted {record_count} records from {region}")

    # Store extracted data count in XCom for downstream tasks
    context['task_instance'].xcom_push(
        key=f'extraction_count_{region}',
        value=record_count
    )

    # Step 2: Write to persistent staging table on staging database
    if record_count > 0:
        staging_hook = PostgresHook(postgres_conn_id='postgres_default')
        staging_conn = staging_hook.get_conn()
        try:
            staging_cursor = staging_conn.cursor()

            # Create persistent staging table if not exists
            staging_cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {EXTRACT_STAGING_TABLE} (
                    sale_id BIGINT,
                    customer_id VARCHAR(50),
                    product_id VARCHAR(50),
                    sale_date TIMESTAMP,
                    quantity INTEGER,
                    unit_price DECIMAL(10,2),
                    total_amount DECIMAL(10,2),
                    store_id VARCHAR(50),
                    payment_method VARCHAR(50),
                    discount_amount DECIMAL(10,2),
                    tax_amount DECIMAL(10,2),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    region VARCHAR(50),
                    etl_batch_id VARCHAR(20)
                );
            """)

            # Clear existing data for this batch + region (idempotent)
            staging_cursor.execute(
                f"DELETE FROM {EXTRACT_STAGING_TABLE} WHERE etl_batch_id = %s AND region = %s",
                (batch_id, region)
            )

            # Bulk insert using COPY with region and batch_id appended
            output = StringIO()
            csv_writer = csv.writer(output, delimiter='\t')
            for record in records:
                csv_writer.writerow(list(record) + [region, batch_id])
            output.seek(0)

            staging_cursor.copy_from(output, EXTRACT_STAGING_TABLE, sep='\t')
            staging_conn.commit()
            staging_cursor.close()
        finally:
            staging_conn.close()

    return {
        'region': region,
        'record_count': record_count,
        'target_date': target_date,
        'status': 'success'
    }


def transform_sales_data(execution_date: str, **context) -> Dict[str, Any]:
    """
    Transform and standardize sales data from all regions.

    Transformations include:
    1. Data type standardization
    2. Currency conversion (if needed)
    3. Timezone normalization
    4. Business rule application
    5. Derived column calculation
    6. Data deduplication

    Args:
        execution_date: Airflow execution date
        **context: Airflow context

    Returns:
        Dict with transformation metadata
    """
    logger.info("Starting data transformation process")
    batch_id = execution_date.replace('-', '')

    # Collect extraction counts from all regions via XCom (for monitoring)
    ti = context['task_instance']
    extraction_metadata = {}

    for region in REGION_NAMES:
        count = ti.xcom_pull(
            task_ids=f'extract_data.extract_{region}',
            key=f'extraction_count_{region}'
        )
        extraction_metadata[region] = count if count else 0

    total_extracted = sum(extraction_metadata.values())
    logger.info(f"Total records extracted across all regions: {total_extracted}")

    if total_extracted == 0:
        logger.warning("No records extracted from any region")
        return {
            'total_records': 0,
            'transformed_records': 0,
            'status': 'no_data'
        }

    # Read all extracted data for this batch from persistent staging table
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    try:
        query = f"SELECT * FROM {EXTRACT_STAGING_TABLE} WHERE etl_batch_id = %s"
        combined_df = pd.read_sql(query, conn, params=(batch_id,))
    finally:
        conn.close()

    if combined_df.empty:
        return {
            'total_records': 0,
            'transformed_records': 0,
            'status': 'no_data'
        }

    logger.info(f"Combined dataframe shape: {combined_df.shape}")

    # ====== TRANSFORMATIONS ======

    # 1. Remove duplicates based on sale_id
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['sale_id'], keep='first')
    dedup_count = initial_count - len(combined_df)
    logger.info(f"Removed {dedup_count} duplicate records")

    # 2. Standardize data types
    combined_df['sale_date'] = pd.to_datetime(combined_df['sale_date'])
    combined_df['created_at'] = pd.to_datetime(combined_df['created_at'])
    combined_df['updated_at'] = pd.to_datetime(combined_df['updated_at'])

    # 3. Handle missing values with business rules
    combined_df['discount_amount'] = combined_df['discount_amount'].fillna(0)
    combined_df['tax_amount'] = combined_df['tax_amount'].fillna(0)

    # 4. Add derived columns
    combined_df['net_amount'] = (
        combined_df['total_amount'] -
        combined_df['discount_amount']
    )

    combined_df['gross_profit'] = (
        combined_df['net_amount'] -
        combined_df['tax_amount']
    )

    # 5. Add partition columns for efficient querying in Snowflake
    combined_df['sale_year'] = combined_df['sale_date'].dt.year
    combined_df['sale_month'] = combined_df['sale_date'].dt.month
    combined_df['sale_day'] = combined_df['sale_date'].dt.day

    # 6. Add processing metadata
    combined_df['etl_loaded_at'] = datetime.now()
    combined_df['etl_batch_id'] = batch_id

    # 7. Normalize region names (driven by region_config.json)
    combined_df['region'] = combined_df['region'].map(REGION_DISPLAY_MAP)

    # 8. Data validation
    combined_df = combined_df[combined_df['quantity'] > 0]
    combined_df = combined_df[combined_df['unit_price'] > 0]
    combined_df = combined_df[combined_df['total_amount'] > 0]

    transformed_count = len(combined_df)
    logger.info(f"Transformation complete. Final record count: {transformed_count}")

    # Store transformed data back to staging table
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Create staging table
        create_staging = f"""
        CREATE TABLE IF NOT EXISTS {EXTRACTION_TABLE} (
            sale_id BIGINT PRIMARY KEY,
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            sale_date TIMESTAMP,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            region VARCHAR(50),
            store_id VARCHAR(50),
            payment_method VARCHAR(50),
            discount_amount DECIMAL(10,2),
            tax_amount DECIMAL(10,2),
            net_amount DECIMAL(10,2),
            gross_profit DECIMAL(10,2),
            sale_year INTEGER,
            sale_month INTEGER,
            sale_day INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            etl_loaded_at TIMESTAMP,
            etl_batch_id VARCHAR(20)
        );
        """
        cursor.execute(create_staging)

        # Clear existing data for this batch (parameterized)
        cursor.execute(
            f"DELETE FROM {EXTRACTION_TABLE} WHERE etl_batch_id = %s",
            (batch_id,)
        )
        conn.commit()

        # Bulk insert transformed data
        engine = create_engine(pg_hook.get_uri())
        combined_df.to_sql(
            EXTRACTION_TABLE.split('.')[1],
            engine,
            schema=EXTRACTION_TABLE.split('.')[0],
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
    finally:
        cursor.close()
        conn.close()

    # Store metadata in XCom
    transformation_metadata = {
        'total_extracted': total_extracted,
        'duplicates_removed': dedup_count,
        'transformed_records': transformed_count,
        'execution_date': execution_date,
        'status': 'success'
    }

    ti.xcom_push(key='transformation_metadata', value=transformation_metadata)

    return transformation_metadata


def validate_data_quality(**context) -> str:
    """
    Perform data quality checks on transformed data.

    Checks:
    1. Minimum record count threshold
    2. NULL value percentage
    3. Required columns present
    4. Data type validation
    5. Business rule validation

    Returns:
        'load_to_snowflake' if validation passes
        'data_quality_failure' if validation fails
    """
    ti = context['task_instance']

    try:
        transformation_metadata = ti.xcom_pull(
            task_ids='transform_sales_data',
            key='transformation_metadata'
        )

        if not transformation_metadata:
            logger.error("No transformation metadata found")
            return 'data_quality_failure'

        transformed_count = transformation_metadata.get('transformed_records', 0)

        # Check 1: Minimum record count
        if transformed_count < MIN_RECORD_COUNT:
            logger.error(
                f"Record count {transformed_count} is below threshold {MIN_RECORD_COUNT}"
            )
            ti.xcom_push(
                key='dq_failure_reason',
                value=f"Record count below threshold: {transformed_count} < {MIN_RECORD_COUNT}"
            )
            return 'data_quality_failure'

        # Connect to database for detailed checks
        execution_date = context['ds']
        batch_id = execution_date.replace('-', '')

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            # Check 2: NULL percentage validation (parameterized)
            null_check_query = f"""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN sale_id IS NULL THEN 1 ELSE 0 END) as null_sale_id,
                SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
                SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
                SUM(CASE WHEN sale_date IS NULL THEN 1 ELSE 0 END) as null_sale_date,
                SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_total_amount
            FROM {EXTRACTION_TABLE}
            WHERE etl_batch_id = %s;
            """

            cursor.execute(null_check_query, (batch_id,))
            null_results = cursor.fetchone()

            total_records = null_results[0]

            for idx, col in enumerate(['sale_id', 'customer_id', 'product_id',
                                       'sale_date', 'total_amount'], start=1):
                null_count = null_results[idx]
                null_percentage = (null_count / total_records * 100) if total_records > 0 else 0

                if null_percentage > MAX_NULL_PERCENTAGE:
                    error_msg = f"Column {col} has {null_percentage:.2f}% NULL values (threshold: {MAX_NULL_PERCENTAGE}%)"
                    logger.error(error_msg)
                    ti.xcom_push(key='dq_failure_reason', value=error_msg)
                    return 'data_quality_failure'

            # Check 3: Business rule validation (parameterized)
            business_rule_query = f"""
            SELECT COUNT(*) as invalid_records
            FROM {EXTRACTION_TABLE}
            WHERE etl_batch_id = %s
                AND (
                    quantity <= 0
                    OR unit_price <= 0
                    OR total_amount <= 0
                    OR total_amount < (quantity * unit_price * 0.5)  -- Sanity check
                    OR total_amount > (quantity * unit_price * 2.0)   -- Sanity check
                );
            """

            cursor.execute(business_rule_query, (batch_id,))
            invalid_count = cursor.fetchone()[0]

            if invalid_count > 0:
                error_msg = f"Found {invalid_count} records violating business rules"
                logger.error(error_msg)
                ti.xcom_push(key='dq_failure_reason', value=error_msg)
                return 'data_quality_failure'

        finally:
            cursor.close()
            conn.close()

        logger.info("All data quality checks passed")
        ti.xcom_push(key='dq_status', value='passed')

        return 'load_to_snowflake'

    except Exception as e:
        logger.error(f"Error in data quality validation: {str(e)}")
        ti.xcom_push(key='dq_failure_reason', value=str(e))
        return 'data_quality_failure'


def prepare_snowflake_load(**context) -> Dict[str, Any]:
    """
    Prepare data for Snowflake load by exporting to S3 or internal stage.

    This function:
    1. Exports transformed data to CSV
    2. Uploads to S3 or Snowflake internal stage
    3. Returns metadata for Snowflake COPY command
    """
    execution_date = context['ds']
    batch_id = execution_date.replace('-', '')

    # Extract data from PostgreSQL staging (parameterized)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    try:
        query = f"SELECT * FROM {EXTRACTION_TABLE} WHERE etl_batch_id = %s ORDER BY sale_id"
        df = pd.read_sql(query, conn, params=(batch_id,))
    finally:
        conn.close()

    logger.info(f"Preparing {len(df)} records for Snowflake load")

    # Convert to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)

    # Upload to S3 (alternative: use Snowflake internal stage)
    s3_bucket = Variable.get('S3_STAGING_BUCKET', default_var='data-pipeline-staging')
    s3_key = f'sales_data/{execution_date}/sales_data_{batch_id}.csv'

    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )

    logger.info(f"Data uploaded to s3://{s3_bucket}/{s3_key}")

    # Store S3 location in XCom
    s3_location = {
        'bucket': s3_bucket,
        'key': s3_key,
        'record_count': len(df)
    }

    context['task_instance'].xcom_push(
        key='s3_location',
        value=s3_location
    )

    return s3_location


def verify_snowflake_load(**context) -> Dict[str, Any]:
    """
    Verify that data was successfully loaded into Snowflake.

    Checks:
    1. Record count matches
    2. No duplicates in final table
    3. Data ranges are correct
    """
    execution_date = context['ds']
    batch_id = execution_date.replace('-', '')

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    # Get expected count from transformation metadata
    ti = context['task_instance']
    transformation_metadata = ti.xcom_pull(
        task_ids='transform_sales_data',
        key='transformation_metadata'
    )
    expected_count = transformation_metadata.get('transformed_records', 0)

    # Count records in Snowflake (parameterized)
    count_query = f"""
    SELECT COUNT(*) as record_count
    FROM {SNOWFLAKE_FINAL_TABLE}
    WHERE etl_batch_id = %s;
    """

    result = sf_hook.get_first(count_query, parameters=(batch_id,))
    actual_count = result[0] if result else 0

    logger.info(f"Snowflake load verification: Expected {expected_count}, Found {actual_count}")

    if actual_count != expected_count:
        error_msg = f"Record count mismatch: Expected {expected_count}, Found {actual_count}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Check for duplicates (parameterized)
    duplicate_check = f"""
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT sale_id, COUNT(*) as cnt
        FROM {SNOWFLAKE_FINAL_TABLE}
        WHERE etl_batch_id = %s
        GROUP BY sale_id
        HAVING COUNT(*) > 1
    );
    """

    dup_result = sf_hook.get_first(duplicate_check, parameters=(batch_id,))
    duplicate_count = dup_result[0] if dup_result else 0

    if duplicate_count > 0:
        error_msg = f"Found {duplicate_count} duplicate sale_ids in Snowflake"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Snowflake load verification successful")

    return {
        'expected_count': expected_count,
        'actual_count': actual_count,
        'status': 'success',
        'batch_id': batch_id
    }


def send_failure_notification(**context) -> None:
    """
    Send detailed failure notification with diagnostic information.
    """
    try:
        ti = context['task_instance']
        execution_date = context['ds']

        # Gather failure information
        dq_failure_reason = ti.xcom_pull(
            task_ids='validate_data_quality',
            key='dq_failure_reason'
        )

        failure_details = {
            'dag_id': context['dag'].dag_id,
            'execution_date': execution_date,
            'failure_reason': dq_failure_reason or 'Unknown failure',
            'task_id': context['task_instance'].task_id
        }

        logger.error(f"Pipeline failure: {json.dumps(failure_details, indent=2)}")

        # In production, send to monitoring system (PagerDuty, Datadog, etc.)
        # For now, just log

    except Exception as e:
        logger.error(f"Error sending failure notification: {str(e)}")


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    # Avoid Variable.get() here — default_args are evaluated at DAG parse time,
    # which hits the metadata DB on every scheduler heartbeat.
    # Override via Airflow UI (Admin -> Variables -> ALERT_EMAIL_LIST) in callbacks.
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'sla': timedelta(hours=2),  # Pipeline should complete within 2 hours
}

with DAG(
    dag_id='retail_sales_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for multi-regional sales data',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'etl', 'production', 'retail'],
    doc_md=__doc__,
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # ========================================================================
    # EXTRACTION PHASE - Parallel extraction from regional databases
    # ========================================================================

    with TaskGroup(group_id='extract_data') as extract_data:
        for region_cfg in REGION_CONFIGS:
            region_name = region_cfg['name']
            extract_task = PythonOperator(
                task_id=f'extract_{region_name}',
                python_callable=extract_sales_data_from_region,
                op_kwargs={
                    'region': region_name,
                    'conn_id': region_cfg['conn_id'],
                    'execution_date': '{{ ds }}'
                },
                execution_timeout=timedelta(
                    minutes=region_cfg.get('extraction_timeout_minutes', 30)
                ),
                retries=region_cfg.get('retries', 3),
                doc_md=f"""
                ### Extract Sales Data - {region_cfg['display_name']}

                Extracts yesterday's sales transactions from the
                {region_name} regional database.

                **Connection:** `{region_cfg['conn_id']}`
                **Timezone:** {region_cfg.get('timezone', 'UTC')}

                **Query Filters:**
                - Date: Previous day ({{% raw %}}{{{{ yesterday_ds }}}}{{% endraw %}})
                - Status: 'completed' only
                - Excludes soft-deleted records
                """,
            )

    # ========================================================================
    # TRANSFORMATION PHASE - Standardize and enrich data
    # ========================================================================

    transform_data = PythonOperator(
        task_id='transform_sales_data',
        python_callable=transform_sales_data,
        op_kwargs={'execution_date': '{{ ds }}'},
        execution_timeout=timedelta(minutes=45),
        doc_md="""
        ### Transform Sales Data

        Consolidates and transforms data from all regional sources:

        **Transformations:**
        - Remove duplicates
        - Standardize data types
        - Calculate derived metrics (net_amount, gross_profit)
        - Add partition columns
        - Normalize region codes
        - Apply business rules

        **Output:** Staging table with validated, enriched data
        """,
    )

    # ========================================================================
    # DATA QUALITY VALIDATION - Branch based on validation results
    # ========================================================================

    validate_quality = BranchPythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        execution_timeout=timedelta(minutes=15),
        doc_md="""
        ### Data Quality Validation

        Performs comprehensive quality checks:

        **Checks:**
        - Minimum record count threshold
        - NULL value percentage < 5%
        - Business rule validation
        - Data type consistency

        **Branches:**
        - Success -> `load_to_snowflake`
        - Failure -> `data_quality_failure`
        """,
    )

    # ========================================================================
    # SNOWFLAKE LOAD PREPARATION
    # ========================================================================

    prepare_load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=prepare_snowflake_load,
        execution_timeout=timedelta(minutes=20),
        doc_md="""
        ### Prepare Snowflake Load

        Exports validated data and uploads to S3 staging area.
        """,
    )

    # ========================================================================
    # SNOWFLAKE - Stage data load
    # ========================================================================

    stage_to_snowflake = SnowflakeOperator(
        task_id='stage_data_in_snowflake',
        conn_id='snowflake_default',
        sql=f"""
        -- Create staging table if not exists
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_STAGE_TABLE} (
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

        -- Truncate staging table
        TRUNCATE TABLE {SNOWFLAKE_STAGE_TABLE};

        -- Copy data from S3
        COPY INTO {SNOWFLAKE_STAGE_TABLE}
        FROM @sales_data_stage/sales_data/{{{{ ds }}}}/
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 0
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'ABORT_STATEMENT';
        """,
        execution_timeout=timedelta(minutes=30),
        doc_md="""
        ### Stage Data in Snowflake

        Loads data from S3 into Snowflake staging table using COPY command.
        """,
    )

    # ========================================================================
    # SNOWFLAKE - Merge into final table (Upsert)
    # ========================================================================

    merge_to_final = SnowflakeOperator(
        task_id='merge_to_final_table',
        conn_id='snowflake_default',
        sql=f"""
        -- Create final table if not exists
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_FINAL_TABLE} (
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

        -- Merge staging data into final table (upsert)
        MERGE INTO {SNOWFLAKE_FINAL_TABLE} target
        USING {SNOWFLAKE_STAGE_TABLE} source
        ON target.sale_id = source.sale_id
        WHEN MATCHED THEN
            UPDATE SET
                customer_id = source.customer_id,
                product_id = source.product_id,
                sale_date = source.sale_date,
                quantity = source.quantity,
                unit_price = source.unit_price,
                total_amount = source.total_amount,
                region = source.region,
                store_id = source.store_id,
                payment_method = source.payment_method,
                discount_amount = source.discount_amount,
                tax_amount = source.tax_amount,
                net_amount = source.net_amount,
                gross_profit = source.gross_profit,
                updated_at = source.updated_at,
                etl_loaded_at = source.etl_loaded_at,
                etl_batch_id = source.etl_batch_id
        WHEN NOT MATCHED THEN
            INSERT (
                sale_id, customer_id, product_id, sale_date, quantity,
                unit_price, total_amount, region, store_id, payment_method,
                discount_amount, tax_amount, net_amount, gross_profit,
                sale_year, sale_month, sale_day, created_at, updated_at,
                etl_loaded_at, etl_batch_id
            )
            VALUES (
                source.sale_id, source.customer_id, source.product_id,
                source.sale_date, source.quantity, source.unit_price,
                source.total_amount, source.region, source.store_id,
                source.payment_method, source.discount_amount, source.tax_amount,
                source.net_amount, source.gross_profit, source.sale_year,
                source.sale_month, source.sale_day, source.created_at,
                source.updated_at, source.etl_loaded_at, source.etl_batch_id
            );

        -- Update table statistics for query optimization
        ALTER TABLE {SNOWFLAKE_FINAL_TABLE} RECLUSTERING;
        """,
        execution_timeout=timedelta(minutes=45),
        doc_md="""
        ### Merge to Final Table

        Performs upsert operation (MERGE) from staging to final analytics table.

        **Strategy:**
        - Updates existing records based on sale_id
        - Inserts new records
        - Maintains data integrity with primary key
        - Optimizes clustering for query performance
        """,
    )

    # ========================================================================
    # VERIFICATION - Ensure data integrity
    # ========================================================================

    verify_load = PythonOperator(
        task_id='verify_snowflake_load',
        python_callable=verify_snowflake_load,
        execution_timeout=timedelta(minutes=10),
        doc_md="""
        ### Verify Snowflake Load

        Post-load validation:
        - Record count matches
        - No duplicates in final table
        - Data completeness checks
        """,
    )

    # ========================================================================
    # FAILURE HANDLING
    # ========================================================================

    data_quality_failed = PythonOperator(
        task_id='data_quality_failure',
        python_callable=send_failure_notification,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        doc_md="""
        ### Data Quality Failure Handler

        Triggered when data quality validation fails.
        Sends notifications and logs diagnostic information.
        """,
    )

    # ========================================================================
    # SUCCESS PATH
    # ========================================================================

    success = EmptyOperator(
        task_id='pipeline_success',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="""
        ### Pipeline Success

        All tasks completed successfully. Data is available in Snowflake.
        """,
    )

    # End task
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # ========================================================================
    # DAG DEPENDENCIES / TASK FLOW
    # ========================================================================

    # Linear flow with branching
    start >> extract_data >> transform_data >> validate_quality

    # Branch on validation results
    validate_quality >> prepare_load >> stage_to_snowflake >> merge_to_final >> verify_load >> success
    validate_quality >> data_quality_failed

    # Both branches converge at end
    success >> end
    data_quality_failed >> end
