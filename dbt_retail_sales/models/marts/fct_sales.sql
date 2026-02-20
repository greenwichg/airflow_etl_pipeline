-- ==============================================================================
-- fct_sales.sql — MART MODEL (Incremental Fact Table)
-- ==============================================================================
--
-- WHAT IS A FACT TABLE?
--   In data warehousing, a FACT TABLE stores business events (transactions).
--   Each row represents one "thing that happened" — here, one sales transaction.
--   Fact tables are typically:
--   - The largest tables in the warehouse (millions to billions of rows)
--   - Joined with dimension tables (dim_regions, dim_stores) for analytics
--   - Named with "fct_" prefix by convention
--
-- WHAT IS INCREMENTAL MATERIALIZATION?
--   Instead of rebuilding the entire table on every run, incremental models
--   only process NEW or CHANGED data since the last run. This is critical
--   for large tables where a full rebuild would take hours.
--
--   HOW IT WORKS:
--   1. First run: dbt creates the table with ALL data (full load)
--   2. Subsequent runs: dbt only inserts/updates rows where
--      etl_loaded_at > MAX(etl_loaded_at) in the existing table
--   3. dbt uses the `is_incremental()` Jinja function to switch behavior
--
--   INCREMENTAL STRATEGIES:
--   - append:         INSERT only (for log/event tables with no updates)
--   - merge:          MERGE INTO (for fact tables with updates) ← we use this
--   - delete+insert:  DELETE matching rows, then INSERT (Snowflake alternative)
--   - insert_overwrite: Replace entire partitions (BigQuery/Spark)
--
-- WHAT IS unique_key?
--   The column(s) dbt uses to match existing rows during MERGE.
--   If a row with this sale_id already exists → UPDATE it.
--   If not → INSERT a new row.
--
-- WHAT IS merge_update_columns?
--   Explicitly lists which columns to UPDATE when a match is found.
--   This prevents accidentally overwriting audit columns like _row_inserted_at.
--
-- WHAT IS cluster_by?
--   Snowflake micro-partition clustering. Tells Snowflake to organize data
--   on disk by these columns, so queries filtering on sale_date and region
--   can skip irrelevant partitions (massive performance improvement).
--
-- FULL REFRESH:
--   To rebuild from scratch: dbt run --full-refresh -s fct_sales
--   This drops and recreates the table, useful when schema changes.
--
-- DATA FLOW:
--   stg_sales_transactions → int_sales_enriched → int_sales_validated → [THIS]
-- ==============================================================================

{{
    config(
        materialized='incremental',           -- Only process new/changed data
        unique_key='sale_id',                 -- Match existing rows by this key
        incremental_strategy='merge',         -- Use SQL MERGE (upsert)

        -- Explicitly list columns to update on MERGE match.
        -- This prevents overwriting _row_inserted_at (which should stay
        -- as the original insert timestamp, not get updated on each merge).
        merge_update_columns=[
            'customer_id', 'product_id', 'store_id', 'sale_date',
            'quantity', 'unit_price', 'total_amount', 'discount_amount',
            'tax_amount', 'net_amount', 'gross_profit', 'region',
            'payment_method', 'sale_year', 'sale_month', 'sale_day',
            'updated_at', 'etl_loaded_at', 'etl_batch_id',
            '_cdc_operation', '_cdc_source_ts', '_row_updated_at'
        ],

        -- Snowflake clustering: organize data on disk by sale_date and region.
        -- This makes queries like "WHERE sale_date = '2024-01-15' AND region = 'US-EAST'"
        -- extremely fast by letting Snowflake skip irrelevant partitions.
        cluster_by=['sale_date', 'region'],

        -- on_schema_change: what to do when the SELECT columns change.
        -- Options: 'ignore' (default), 'fail', 'append_new_columns', 'sync_all_columns'
        -- 'append_new_columns' = if you add a new column to the SELECT, dbt will
        -- ALTER TABLE ADD COLUMN automatically. Existing rows get NULL for the new column.
        -- This prevents the need for --full-refresh when evolving the schema.
        on_schema_change='append_new_columns',

        -- Tags for selective execution
        tags=['marts', 'daily', 'fact']
    )
}}

/*
    Fact table: validated sales transactions.

    Incremental strategy:
    - Full refresh: loads all validated records
    - Incremental: only processes new/changed records since last run
      (filtered by etl_loaded_at)

    CDC deletes are handled in a post-hook (see cdc_delete_post_hook macro).
*/

-- =============================================================================
-- SOURCE CTE — Read validated data with optional incremental filter.
-- =============================================================================
with validated as (

    select * from {{ ref('int_sales_validated') }}

    -- =========================================================================
    -- INCREMENTAL FILTER — The heart of incremental models.
    -- =========================================================================
    -- is_incremental() is a Jinja function that returns:
    --   - FALSE on the first run (table doesn't exist yet → load everything)
    --   - FALSE when running with --full-refresh flag
    --   - TRUE on subsequent runs (table exists → only load new data)
    --
    -- {{ this }} refers to THIS model's existing table in the database.
    -- We query it to find the MAX(etl_loaded_at) — the "watermark" — and
    -- only process rows newer than that watermark.
    --
    -- COALESCE handles the edge case where the table is empty (returns '1900-01-01').
    --
    -- LEARNING TIP:
    --   This is the most common incremental pattern:
    --   WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
    {% if is_incremental() %}
    -- Only process records loaded since the last dbt run
    where etl_loaded_at > (select coalesce(max(etl_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}

)

-- =============================================================================
-- FINAL SELECT — Explicit column list (no SELECT * in mart models).
-- =============================================================================
-- BEST PRACTICE: Always list columns explicitly in mart models.
-- This serves as documentation and prevents accidental schema changes.
select
    -- ===== Primary key =====
    sale_id,

    -- ===== Foreign keys (for joining with dimension tables) =====
    customer_id,
    product_id,
    store_id,

    -- ===== Timestamps =====
    sale_date,
    created_at,
    updated_at,

    -- ===== Measures (numeric values for aggregation) =====
    quantity,
    unit_price,
    total_amount,
    discount_amount,
    tax_amount,
    net_amount,              -- Derived in int_sales_enriched
    gross_profit,            -- Derived in int_sales_enriched

    -- ===== Dimensions (for filtering and grouping) =====
    region,
    payment_method,

    -- ===== Partition columns (for Snowflake clustering) =====
    sale_year,
    sale_month,
    sale_day,

    -- ===== CDC metadata (for debugging and auditing) =====
    _cdc_operation,          -- c=create, u=update, d=delete
    _cdc_source_ts,          -- When the change happened in the source DB

    -- ===== ETL metadata =====
    etl_batch_id,            -- Which Airflow batch loaded this row
    etl_loaded_at,           -- When Airflow loaded this row into staging

    -- ===== dbt metadata (audit columns) =====
    -- _row_inserted_at: set on INSERT, never updated (thanks to merge_update_columns)
    -- _row_updated_at: set on every MERGE (INSERT or UPDATE)
    current_timestamp()     as _row_inserted_at,
    current_timestamp()     as _row_updated_at

from validated
