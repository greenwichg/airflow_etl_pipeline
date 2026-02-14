-- =============================================================================
-- Snowflake Streams — Change Data Capture on Staging Tables
-- =============================================================================
-- Streams track row-level changes (INSERTs into the staging tables) so that
-- downstream Tasks can consume only the new/changed data incrementally.
--
-- How it works:
--   1. Kafka Connector writes raw CDC events -> STAGING.CDC_SALES_EVENTS
--   2. Stream A (on CDC_SALES_EVENTS) feeds Task A which parses events
--      into STAGING.SALES_DATA_STAGE
--   3. Stream B (on SALES_DATA_STAGE) feeds Task B which MERGEs parsed
--      rows into ANALYTICS.SALES_FACT
--
-- This two-stage pipeline gives us an auditable parsed staging layer and
-- keeps the final MERGE simple and efficient.
-- =============================================================================

USE ROLE ETL_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL_DW;

-- ---------------------------------------------------------------------------
-- Stream A: Track new raw CDC events arriving from Kafka
-- ---------------------------------------------------------------------------
-- APPEND_ONLY = TRUE because the Kafka connector only inserts rows (never
-- updates or deletes in the raw events table).

CREATE STREAM IF NOT EXISTS STAGING.STREAM_CDC_RAW_EVENTS
    ON TABLE STAGING.CDC_SALES_EVENTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures new Debezium CDC events for parsing';

-- ---------------------------------------------------------------------------
-- Stream B: Track parsed/staged rows ready for final merge
-- ---------------------------------------------------------------------------
-- APPEND_ONLY = TRUE because the parse task only inserts into the staging table.

CREATE STREAM IF NOT EXISTS STAGING.STREAM_SALES_STAGED
    ON TABLE STAGING.SALES_DATA_STAGE
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Captures parsed CDC rows for merge into analytics.sales_fact';

-- ---------------------------------------------------------------------------
-- Verification queries
-- ---------------------------------------------------------------------------
-- Check stream metadata:
--   SELECT SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_CDC_RAW_EVENTS');
--   SELECT SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_SALES_STAGED');
--
-- Preview unconsumed changes:
--   SELECT * FROM STAGING.STREAM_CDC_RAW_EVENTS LIMIT 10;
--   SELECT * FROM STAGING.STREAM_SALES_STAGED LIMIT 10;
