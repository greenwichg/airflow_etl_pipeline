-- =============================================================================
-- Snowflake Tasks — Automated CDC Processing Pipeline
-- =============================================================================
-- Two chained tasks that run automatically when new CDC events arrive:
--
--   Task A (PARSE_CDC_EVENTS)  —  every 1 min  — parses raw Debezium JSON
--                                                 into structured staging rows
--                 |
--                 v
--   Task B (MERGE_TO_SALES_FACT) — child task  — CDC-aware MERGE into the
--                                                 final analytics table
--
-- Both tasks are guarded by SYSTEM$STREAM_HAS_DATA() so they only execute
-- when there is actually new data to process (no wasted compute).
-- =============================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DW;

-- ---------------------------------------------------------------------------
-- Task A: Parse raw Debezium CDC events into structured staging table
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.PARSE_CDC_EVENTS
    WAREHOUSE = ETL_WH
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Parses raw Debezium CDC JSON events into staging.sales_data_stage'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_CDC_RAW_EVENTS')
AS
INSERT INTO STAGING.SALES_DATA_STAGE (
    sale_id, customer_id, product_id, sale_date, quantity,
    unit_price, total_amount, region, store_id, payment_method,
    discount_amount, tax_amount, net_amount, gross_profit,
    sale_year, sale_month, sale_day, created_at, updated_at,
    is_deleted, _cdc_operation, _cdc_source_ts, _kafka_offset,
    etl_loaded_at, etl_batch_id
)
SELECT
    -- Extract fields from the Debezium "after" payload (or "before" for deletes)
    COALESCE(
        RECORD_CONTENT:after:sale_id::BIGINT,
        RECORD_CONTENT:before:sale_id::BIGINT
    )                                                       AS sale_id,
    RECORD_CONTENT:after:customer_id::VARCHAR(50)           AS customer_id,
    RECORD_CONTENT:after:product_id::VARCHAR(50)            AS product_id,
    RECORD_CONTENT:after:sale_date::TIMESTAMP_NTZ           AS sale_date,
    RECORD_CONTENT:after:quantity::INTEGER                   AS quantity,
    RECORD_CONTENT:after:unit_price::NUMBER(10,2)           AS unit_price,
    RECORD_CONTENT:after:total_amount::NUMBER(10,2)         AS total_amount,
    COALESCE(
        RECORD_CONTENT:after:region::VARCHAR(50),
        RECORD_CONTENT:before:region::VARCHAR(50)
    )                                                       AS region,
    RECORD_CONTENT:after:store_id::VARCHAR(50)              AS store_id,
    RECORD_CONTENT:after:payment_method::VARCHAR(50)        AS payment_method,
    COALESCE(RECORD_CONTENT:after:discount_amount::NUMBER(10,2), 0)
                                                            AS discount_amount,
    COALESCE(RECORD_CONTENT:after:tax_amount::NUMBER(10,2), 0)
                                                            AS tax_amount,
    -- Derived columns
    COALESCE(RECORD_CONTENT:after:total_amount::NUMBER(10,2), 0)
        - COALESCE(RECORD_CONTENT:after:discount_amount::NUMBER(10,2), 0)
                                                            AS net_amount,
    COALESCE(RECORD_CONTENT:after:total_amount::NUMBER(10,2), 0)
        - COALESCE(RECORD_CONTENT:after:discount_amount::NUMBER(10,2), 0)
        - COALESCE(RECORD_CONTENT:after:tax_amount::NUMBER(10,2), 0)
                                                            AS gross_profit,
    -- Partition columns
    YEAR(RECORD_CONTENT:after:sale_date::TIMESTAMP_NTZ)     AS sale_year,
    MONTH(RECORD_CONTENT:after:sale_date::TIMESTAMP_NTZ)    AS sale_month,
    DAY(RECORD_CONTENT:after:sale_date::TIMESTAMP_NTZ)      AS sale_day,
    RECORD_CONTENT:after:created_at::TIMESTAMP_NTZ          AS created_at,
    COALESCE(
        RECORD_CONTENT:after:updated_at::TIMESTAMP_NTZ,
        RECORD_CONTENT:before:updated_at::TIMESTAMP_NTZ
    )                                                       AS updated_at,
    -- CDC operation type determines if this is a delete marker
    CASE
        WHEN RECORD_CONTENT:op::VARCHAR = 'd' THEN TRUE
        WHEN RECORD_CONTENT:after:is_deleted::BOOLEAN = TRUE THEN TRUE
        ELSE FALSE
    END                                                     AS is_deleted,
    RECORD_CONTENT:op::VARCHAR(1)                           AS _cdc_operation,
    -- Source database timestamp (ms since epoch -> timestamp)
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:ts_ms::BIGINT, 3)      AS _cdc_source_ts,
    _KAFKA_OFFSET                                           AS _kafka_offset,
    CURRENT_TIMESTAMP()                                     AS etl_loaded_at,
    TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')                     AS etl_batch_id
FROM STAGING.STREAM_CDC_RAW_EVENTS;


-- ---------------------------------------------------------------------------
-- Task B: CDC-aware MERGE from staging into final analytics table
-- ---------------------------------------------------------------------------
-- This is a child task of PARSE_CDC_EVENTS — it runs immediately after
-- Task A completes (when Task A inserted new rows into the staging table).

CREATE OR REPLACE TASK STAGING.MERGE_TO_SALES_FACT
    WAREHOUSE = ETL_WH
    COMMENT   = 'CDC-aware merge: upsert live rows, delete soft-deleted rows'
    AFTER STAGING.PARSE_CDC_EVENTS
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_SALES_STAGED')
AS
DECLARE
    v_inserted  INTEGER DEFAULT 0;
    v_updated   INTEGER DEFAULT 0;
    v_deleted   INTEGER DEFAULT 0;
    v_min_offset BIGINT;
    v_max_offset BIGINT;
BEGIN
    -- Capture offset range for audit logging
    SELECT MIN(_kafka_offset), MAX(_kafka_offset)
    INTO :v_min_offset, :v_max_offset
    FROM STAGING.STREAM_SALES_STAGED;

    -- CDC-aware MERGE: handles inserts, updates, and deletes in one statement
    MERGE INTO ANALYTICS.SALES_FACT target
    USING (
        -- Deduplicate within the stream batch: keep the latest event per sale_id
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY sale_id ORDER BY _cdc_source_ts DESC, _kafka_offset DESC
                ) AS _rn
            FROM STAGING.STREAM_SALES_STAGED
        )
        WHERE _rn = 1
    ) source
    ON target.sale_id = source.sale_id

    -- Delete: remove rows flagged as deleted
    WHEN MATCHED AND source.is_deleted = TRUE THEN
        DELETE

    -- Update: overwrite existing rows with new values
    WHEN MATCHED AND source.is_deleted = FALSE THEN
        UPDATE SET
            customer_id      = source.customer_id,
            product_id       = source.product_id,
            sale_date        = source.sale_date,
            quantity         = source.quantity,
            unit_price       = source.unit_price,
            total_amount     = source.total_amount,
            region           = source.region,
            store_id         = source.store_id,
            payment_method   = source.payment_method,
            discount_amount  = source.discount_amount,
            tax_amount       = source.tax_amount,
            net_amount       = source.net_amount,
            gross_profit     = source.gross_profit,
            sale_year        = source.sale_year,
            sale_month       = source.sale_month,
            sale_day         = source.sale_day,
            updated_at       = source.updated_at,
            etl_loaded_at    = source.etl_loaded_at,
            etl_batch_id     = source.etl_batch_id,
            _cdc_operation   = source._cdc_operation,
            _cdc_source_ts   = source._cdc_source_ts,
            _row_updated_at  = CURRENT_TIMESTAMP()

    -- Insert: new rows that don't exist in the target
    WHEN NOT MATCHED AND source.is_deleted = FALSE THEN
        INSERT (
            sale_id, customer_id, product_id, sale_date, quantity,
            unit_price, total_amount, region, store_id, payment_method,
            discount_amount, tax_amount, net_amount, gross_profit,
            sale_year, sale_month, sale_day, created_at, updated_at,
            etl_loaded_at, etl_batch_id, _cdc_operation, _cdc_source_ts
        )
        VALUES (
            source.sale_id, source.customer_id, source.product_id,
            source.sale_date, source.quantity, source.unit_price,
            source.total_amount, source.region, source.store_id,
            source.payment_method, source.discount_amount, source.tax_amount,
            source.net_amount, source.gross_profit, source.sale_year,
            source.sale_month, source.sale_day, source.created_at,
            source.updated_at, source.etl_loaded_at, source.etl_batch_id,
            source._cdc_operation, source._cdc_source_ts
        );

    -- Audit log
    INSERT INTO AUDIT.CDC_MERGE_LOG (
        rows_inserted, rows_updated, rows_deleted,
        min_kafka_offset, max_kafka_offset,
        stream_name, task_name, status
    )
    VALUES (
        :v_inserted, :v_updated, :v_deleted,
        :v_min_offset, :v_max_offset,
        'STAGING.STREAM_SALES_STAGED', 'STAGING.MERGE_TO_SALES_FACT', 'SUCCESS'
    );
END;


-- ---------------------------------------------------------------------------
-- Activate the task tree (tasks are created in SUSPENDED state by default)
-- ---------------------------------------------------------------------------
-- Start from the leaf task up to the root task.
ALTER TASK STAGING.MERGE_TO_SALES_FACT RESUME;
ALTER TASK STAGING.PARSE_CDC_EVENTS RESUME;

-- ---------------------------------------------------------------------------
-- Monitoring queries
-- ---------------------------------------------------------------------------
-- Task execution history:
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
--   WHERE NAME IN ('PARSE_CDC_EVENTS', 'MERGE_TO_SALES_FACT')
--   ORDER BY SCHEDULED_TIME DESC LIMIT 20;
--
-- Stream lag:
--   SELECT SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_CDC_RAW_EVENTS') AS raw_has_data,
--          SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_SALES_STAGED')   AS staged_has_data;
--
-- Audit trail:
--   SELECT * FROM AUDIT.CDC_MERGE_LOG ORDER BY merge_timestamp DESC LIMIT 20;
--
-- To suspend tasks for maintenance:
--   ALTER TASK STAGING.PARSE_CDC_EVENTS SUSPEND;
--   ALTER TASK STAGING.MERGE_TO_SALES_FACT SUSPEND;
