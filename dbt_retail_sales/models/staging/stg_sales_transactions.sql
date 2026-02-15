-- ==============================================================================
-- stg_sales_transactions.sql — STAGING MODEL (View)
-- ==============================================================================
--
-- WHAT IS A STAGING MODEL?
--   Staging models are the FIRST layer in dbt. They sit directly on top of
--   raw source tables and do three things:
--   1. Clean: fix data types, handle NULLs, rename columns
--   2. Deduplicate: remove duplicate records from the source
--   3. Filter: separate live rows from delete markers
--
-- WHY A VIEW?
--   Views don't store data — they're just saved SQL queries. This means:
--   - Zero storage cost (the data lives only in the source table)
--   - Always fresh (queries always read the latest source data)
--   - Fast to build (no data copying needed)
--   Use views for staging because the data is read-through, not aggregated.
--
-- DATA FLOW:
--   source('raw_cdc', 'sales_data_stage') → this model → int_sales_enriched
--
-- NAMING CONVENTION:
--   stg_<source>_<entity>  →  stg_sales_transactions
--   (stg = staging, entity = what the data represents)
-- ==============================================================================

-- =============================================================================
-- CONFIG BLOCK — Model-level configuration.
-- =============================================================================
-- The config() block overrides defaults from dbt_project.yml for THIS model.
-- In practice, these match the folder defaults, but we declare them explicitly
-- here for clarity and self-documentation.
--
-- LEARNING TIP:
--   You can configure materialization, tags, schema, etc. either:
--   1. In dbt_project.yml (folder-level defaults) — preferred for consistency
--   2. In the model's config() block — for model-specific overrides
--   3. In the YAML file — also valid, keeps SQL clean
{{
    config(
        materialized='view',        -- Creates a VIEW in the database (no data stored)
        tags=['staging', 'daily']   -- Tags for selective execution: dbt run -s tag:daily
    )
}}

/*
    Staging model: clean, cast, and deduplicate raw sales transactions.

    Business rules applied here:
    - Deduplication by sale_id (keep latest by updated_at)
    - NULL defaults for discount_amount and tax_amount
    - Type casting to canonical types
    - Filters out delete markers (handled by stg_cdc_delete_markers)
*/

-- =============================================================================
-- CTE 1: source — Read from the raw source table.
-- =============================================================================
-- source() is dbt's way of referencing raw tables defined in _sources.yml.
-- Unlike ref(), source() doesn't create a DAG dependency between dbt models —
-- it just points to an external table.
--
-- LEARNING TIP:
--   ALWAYS use source() for raw tables and ref() for dbt models.
--   source() → external tables (loaded by Airflow/Debezium/Fivetran)
--   ref()    → other dbt models (staging, intermediate, marts)
with source as (

    select * from {{ source('raw_cdc', 'sales_data_stage') }}
    -- Filter out rows with no batch ID (malformed or test data)
    where etl_batch_id is not null

),

-- =============================================================================
-- CTE 2: live_rows — Separate live records from CDC delete markers.
-- =============================================================================
-- CDC (Change Data Capture) pipelines send BOTH:
--   - Live rows (is_deleted = false): inserts and updates
--   - Delete markers (is_deleted = true): records deleted in the source DB
--
-- We split them at the staging layer because they need different handling:
--   - Live rows → flow through enrichment → fact table (this model)
--   - Delete markers → stg_cdc_delete_markers → used by post_hook to DELETE
live_rows as (

    select *
    from source
    where is_deleted = false

),

-- =============================================================================
-- CTE 3: deduplicated — Remove duplicate sale_id records.
-- =============================================================================
-- WHY DEDUPLICATION?
--   The same sale_id can appear multiple times because:
--   1. A sale was updated (e.g., price correction) → multiple CDC events
--   2. Overlapping batch windows extracted the same row twice
--   3. Retry logic in Airflow re-extracted a batch
--
-- HOW IT WORKS:
--   ROW_NUMBER() assigns a sequence number within each sale_id group,
--   ordered by updated_at DESC (most recent first). Then we keep only
--   _row_num = 1 (the latest version of each sale).
--
-- LEARNING TIP:
--   This is the most common deduplication pattern in SQL/dbt:
--   ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC) = 1
deduplicated as (

    select
        *,
        row_number() over (
            partition by sale_id                              -- Group by primary key
            order by updated_at desc, _cdc_source_ts desc nulls last  -- Latest first
        ) as _row_num
    from live_rows

),

-- =============================================================================
-- CTE 4: cleaned — Final column selection with type casting and null handling.
-- =============================================================================
-- BEST PRACTICES applied here:
--   1. Explicit column selection (no SELECT * in staging output)
--   2. Type casting with ::type syntax (Snowflake-specific shorthand for CAST)
--   3. COALESCE for null defaults (discount/tax default to 0, not NULL)
--   4. Logical grouping with comments (keys, timestamps, measures, dimensions)
--
-- LEARNING TIP:
--   Always cast types explicitly in staging models. Raw sources often have
--   VARCHAR columns that should be INTEGER or TIMESTAMP. Catching type issues
--   here prevents failures in downstream models.
cleaned as (

    select
        -- Keys (identifiers)
        sale_id,
        customer_id,
        product_id,
        store_id,

        -- Timestamps (cast to TIMESTAMP_NTZ = no timezone, for consistency)
        sale_date::timestamp_ntz                        as sale_date,
        created_at::timestamp_ntz                       as created_at,
        updated_at::timestamp_ntz                       as updated_at,

        -- Measures (numeric values with explicit precision)
        -- ::number(10, 2) means: 10 total digits, 2 decimal places
        -- COALESCE(column, 0) replaces NULL with 0 (common for optional amounts)
        quantity::integer                                as quantity,
        unit_price::number(10, 2)                       as unit_price,
        total_amount::number(10, 2)                     as total_amount,
        coalesce(discount_amount, 0)::number(10, 2)     as discount_amount,
        coalesce(tax_amount, 0)::number(10, 2)          as tax_amount,

        -- Dimensions (categorical attributes)
        region,
        payment_method,

        -- CDC metadata (passed through for downstream models to use)
        is_deleted,
        _cdc_operation,
        _cdc_source_ts,

        -- ETL metadata (for auditing and batch tracking)
        etl_batch_id,
        etl_loaded_at

    from deduplicated
    where _row_num = 1    -- Keep only the latest version of each sale_id

)

-- Final output: clean, deduplicated, typed records ready for enrichment
select * from cleaned
