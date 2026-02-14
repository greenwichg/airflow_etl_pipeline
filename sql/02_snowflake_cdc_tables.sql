-- =============================================================================
-- Snowflake CDC Tables Setup
-- =============================================================================
-- Creates the tables that receive CDC events from Kafka (via Snowpipe Streaming)
-- and the final analytics table consumed by BI tools.
--
-- Run order: 02 -> 03 (streams) -> 04 (tasks)
-- =============================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DW;

-- ---------------------------------------------------------------------------
-- 1. Raw CDC events landing table (populated by Snowflake Kafka Connector)
-- ---------------------------------------------------------------------------
-- Each row is a Debezium change event: INSERT, UPDATE, or DELETE.
-- The Kafka connector writes here via Snowpipe Streaming.

CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE TABLE IF NOT EXISTS STAGING.CDC_SALES_EVENTS (
    -- Debezium envelope fields
    RECORD_CONTENT       VARIANT    NOT NULL,    -- Full Debezium JSON envelope
    RECORD_METADATA      VARIANT    NOT NULL,    -- Kafka metadata (topic, partition, offset)
    -- Extraction helpers (populated by Snowflake Kafka connector)
    _KAFKA_TOPIC         VARCHAR(256),
    _KAFKA_PARTITION     INTEGER,
    _KAFKA_OFFSET        BIGINT,
    _KAFKA_TIMESTAMP     TIMESTAMP_NTZ,
    _INSERTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 2. Parsed CDC staging table (materialized from raw events by Stream + Task)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS STAGING.SALES_DATA_STAGE (
    sale_id              BIGINT       NOT NULL,
    customer_id          VARCHAR(50),
    product_id           VARCHAR(50),
    sale_date            TIMESTAMP_NTZ,
    quantity             INTEGER,
    unit_price           NUMBER(10,2),
    total_amount         NUMBER(10,2),
    region               VARCHAR(50),
    store_id             VARCHAR(50),
    payment_method       VARCHAR(50),
    discount_amount      NUMBER(10,2),
    tax_amount           NUMBER(10,2),
    net_amount           NUMBER(10,2),
    gross_profit         NUMBER(10,2),
    sale_year            INTEGER,
    sale_month           INTEGER,
    sale_day             INTEGER,
    created_at           TIMESTAMP_NTZ,
    updated_at           TIMESTAMP_NTZ,
    is_deleted           BOOLEAN      DEFAULT FALSE,
    etl_loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_batch_id         VARCHAR(20),
    _cdc_operation       VARCHAR(1),  -- 'c'reate, 'u'pdate, 'd'elete, 'r'ead (snapshot)
    _cdc_source_ts       TIMESTAMP_NTZ,
    _kafka_offset        BIGINT
);

-- ---------------------------------------------------------------------------
-- 3. Final analytics fact table
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

CREATE TABLE IF NOT EXISTS ANALYTICS.SALES_FACT (
    sale_id              BIGINT       PRIMARY KEY,
    customer_id          VARCHAR(50),
    product_id           VARCHAR(50),
    sale_date            TIMESTAMP_NTZ,
    quantity             INTEGER,
    unit_price           NUMBER(10,2),
    total_amount         NUMBER(10,2),
    region               VARCHAR(50),
    store_id             VARCHAR(50),
    payment_method       VARCHAR(50),
    discount_amount      NUMBER(10,2),
    tax_amount           NUMBER(10,2),
    net_amount           NUMBER(10,2),
    gross_profit         NUMBER(10,2),
    sale_year            INTEGER,
    sale_month           INTEGER,
    sale_day             INTEGER,
    created_at           TIMESTAMP_NTZ,
    updated_at           TIMESTAMP_NTZ,
    etl_loaded_at        TIMESTAMP_NTZ,
    etl_batch_id         VARCHAR(20),
    _cdc_operation       VARCHAR(1),
    _cdc_source_ts       TIMESTAMP_NTZ,
    _row_inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _row_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (sale_date, region);

-- ---------------------------------------------------------------------------
-- 4. CDC audit / lineage table (tracks every merge cycle)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS AUDIT;

CREATE TABLE IF NOT EXISTS AUDIT.CDC_MERGE_LOG (
    merge_id             BIGINT AUTOINCREMENT,
    merge_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rows_inserted        INTEGER,
    rows_updated         INTEGER,
    rows_deleted         INTEGER,
    min_kafka_offset     BIGINT,
    max_kafka_offset     BIGINT,
    stream_name          VARCHAR(256),
    task_name            VARCHAR(256),
    status               VARCHAR(20)  -- 'SUCCESS' | 'FAILED'
);
