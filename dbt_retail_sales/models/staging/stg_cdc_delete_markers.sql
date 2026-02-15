-- ==============================================================================
-- stg_cdc_delete_markers.sql — STAGING MODEL (View)
-- ==============================================================================
--
-- WHAT DOES THIS MODEL DO?
--   Extracts CDC (Change Data Capture) delete markers from the staging table.
--   When a record is deleted in the source PostgreSQL database, the CDC pipeline
--   sends a "soft delete" event with is_deleted = TRUE. This model collects
--   those delete markers so downstream models can remove the corresponding rows.
--
-- WHY A SEPARATE MODEL?
--   The staging table contains BOTH live rows and delete markers mixed together.
--   Splitting them into separate models follows the Single Responsibility Principle:
--   - stg_sales_transactions: handles live rows (inserts + updates)
--   - stg_cdc_delete_markers: handles deletes
--
-- HOW DELETE MARKERS ARE USED:
--   The fct_sales mart model uses a "post_hook" (SQL that runs AFTER the model):
--     DELETE FROM fct_sales WHERE sale_id IN (SELECT sale_id FROM this_model)
--   This ensures deleted records are removed from the fact table.
--
-- DATA FLOW:
--   source('raw_cdc', 'sales_data_stage') → this model → fct_sales (post_hook)
--
-- LEARNING TIP:
--   In CDC pipelines, handling deletes is the hardest part. There are 3 approaches:
--   1. Soft delete flag (is_deleted column) ← what we use here
--   2. Debezium tombstone events (op = 'd')
--   3. Snowflake Streams (automatically track DML changes)
-- ==============================================================================

{{
    config(
        materialized='view',     -- View: no storage cost, always fresh
        tags=['staging', 'cdc']  -- Tagged for CDC-specific runs: dbt run -s tag:cdc
    )
}}

/*
    Extracts CDC delete markers from the staging table.

    These are rows where is_deleted = TRUE, meaning the source system
    soft-deleted the transaction. The incremental mart model uses these
    to remove corresponding rows from the fact table.
*/

-- SELECT DISTINCT ensures each sale_id appears only once in the delete list,
-- even if multiple delete events were captured for the same record.
select distinct
    sale_id,                -- The primary key to delete from the fact table
    region,                 -- Included for auditing (which region's data was deleted)
    updated_at,             -- When the delete occurred in the source system
    _cdc_operation,         -- Should be 'd' for delete operations
    _cdc_source_ts,         -- Debezium's source timestamp of the delete event
    etl_batch_id            -- Which batch captured this delete marker
from {{ source('raw_cdc', 'sales_data_stage') }}
where is_deleted = true         -- Only keep delete markers
  and etl_batch_id is not null  -- Exclude malformed records
