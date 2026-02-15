-- ==============================================================================
-- int_sales_enriched.sql — INTERMEDIATE MODEL (Ephemeral)
-- ==============================================================================
--
-- WHAT IS AN INTERMEDIATE MODEL?
--   Intermediate models contain BUSINESS LOGIC — derived calculations, joins,
--   and enrichments that transform clean staging data into analytics-ready output.
--   They sit between staging (raw cleaning) and marts (final output).
--
-- WHAT IS "EPHEMERAL" MATERIALIZATION?
--   Ephemeral models do NOT create any table or view in the database.
--   Instead, dbt inlines them as a CTE (Common Table Expression) inside the
--   downstream model that references them. Think of it like a function call
--   that gets expanded inline at compile time.
--
--   Example: When fct_sales does {{ ref('int_sales_validated') }}, and
--   int_sales_validated does {{ ref('int_sales_enriched') }}, dbt compiles
--   ALL of this into a single SQL query with nested CTEs. No intermediate
--   tables are created in the database.
--
-- WHY USE EPHEMERAL?
--   - Zero storage cost (no table/view in the warehouse)
--   - Keeps the warehouse clean (fewer objects to manage)
--   - Organizes complex logic into readable, testable steps
--   - BUT: you can't query ephemeral models directly in the warehouse
--     (they only exist as compiled SQL inside downstream models)
--
-- WHEN NOT TO USE EPHEMERAL:
--   - If you need to query the intermediate result for debugging
--   - If multiple downstream models reference it (it gets duplicated as a CTE
--     in each one, potentially causing redundant computation)
--   - If the logic is expensive and should be materialized once
--
-- DATA FLOW:
--   stg_sales_transactions → [THIS MODEL] → int_sales_validated → fct_sales
--
-- NAMING CONVENTION:
--   int_<entity>_<verb>  →  int_sales_enriched
--   (int = intermediate, verb = what this model does to the data)
-- ==============================================================================

-- Ephemeral: this model is NOT materialized in the database.
-- It becomes an inline CTE in the downstream model (int_sales_validated).
{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Enrich staged sales with derived business metrics.

    Calculations:
    - net_amount = total_amount - discount_amount
    - gross_profit = net_amount - tax_amount
    - Partition columns: sale_year, sale_month, sale_day
    - Amount sanity check flag for downstream filtering
*/

-- =============================================================================
-- CTE 1: staged — Read from the staging model using ref().
-- =============================================================================
-- ref() creates a DEPENDENCY in the dbt DAG:
--   "This model depends on stg_sales_transactions"
-- dbt uses this to determine the correct build order:
--   stg_sales_transactions MUST run before int_sales_enriched.
--
-- LEARNING TIP:
--   ref() is the MOST IMPORTANT function in dbt. It does two things:
--   1. Resolves the model name to its full database path (schema.table)
--   2. Tells dbt about the dependency for correct execution order
with staged as (

    select * from {{ ref('stg_sales_transactions') }}

),

-- =============================================================================
-- CTE 2: enriched — Add derived columns and business calculations.
-- =============================================================================
-- This is where business logic lives. We compute metrics that don't exist
-- in the raw data but are needed for analytics:
--   - net_amount: what the customer actually paid (after discounts)
--   - gross_profit: revenue minus tax
--   - Partition columns: for Snowflake clustering (faster queries)
--   - Sanity check: flags suspicious transactions for investigation
enriched as (

    select
        -- ===== Keys (pass through unchanged) =====
        sale_id,
        customer_id,
        product_id,
        store_id,

        -- ===== Timestamps (pass through unchanged) =====
        sale_date,
        created_at,
        updated_at,

        -- ===== Original measures (pass through unchanged) =====
        quantity,
        unit_price,
        total_amount,
        discount_amount,
        tax_amount,

        -- ===== DERIVED measures (calculated from original columns) =====

        -- Net amount: what the customer paid after discounts
        -- Formula: total_amount - discount_amount
        -- ::number(10, 2) ensures consistent decimal precision
        (total_amount - discount_amount)::number(10, 2)     as net_amount,

        -- Gross profit: revenue after both discounts and taxes
        -- This is the actual money the business keeps
        (total_amount - discount_amount - tax_amount)::number(10, 2)
                                                            as gross_profit,

        -- ===== PARTITION columns (extracted from sale_date) =====
        -- These are used for Snowflake micro-partition clustering.
        -- Clustering by date components dramatically speeds up queries like:
        --   WHERE sale_year = 2024 AND sale_month = 1
        -- Snowflake can skip entire micro-partitions that don't match.
        year(sale_date)                                     as sale_year,
        month(sale_date)                                    as sale_month,
        day(sale_date)                                      as sale_day,

        -- ===== Dimensions (pass through) =====
        region,
        payment_method,

        -- ===== BUSINESS RULE: amount sanity check =====
        -- Validates that total_amount is reasonable relative to qty * unit_price.
        -- A valid total should be between 50% and 200% of the expected amount.
        -- Why not exactly equal? Because of rounding, bulk discounts, promotions, etc.
        -- Rows failing this check are flagged (not removed) — the next model filters them.
        case
            when total_amount >= (quantity * unit_price * 0.5)
             and total_amount <= (quantity * unit_price * 2.0)
            then true
            else false
        end                                                 as is_amount_valid,

        -- ===== CDC metadata (pass through for downstream use) =====
        is_deleted,
        _cdc_operation,
        _cdc_source_ts,

        -- ===== ETL metadata (pass through for auditing) =====
        etl_batch_id,
        etl_loaded_at

    from staged

)

select * from enriched
