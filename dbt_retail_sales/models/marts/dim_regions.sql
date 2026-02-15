-- ==============================================================================
-- dim_regions.sql — MART MODEL (Dimension Table)
-- ==============================================================================
--
-- WHAT IS A DIMENSION TABLE?
--   In data warehousing, DIMENSION TABLES describe the "who/what/where/when"
--   of business events. They provide context for fact table measures:
--   - dim_regions: WHERE did the sale happen? (region info)
--   - dim_stores:  WHICH store made the sale? (store info)
--   - dim_customers: WHO bought it? (customer info)
--
--   Dimension tables are:
--   - Small relative to fact tables (4 rows vs millions)
--   - Joined to fact tables via foreign keys
--   - Named with "dim_" prefix by convention
--
-- WHY MATERIALIZED AS TABLE (NOT INCREMENTAL)?
--   This dimension is small (one row per region = 4 rows) and is derived
--   from the fact table. A full rebuild takes milliseconds, so incremental
--   processing would add complexity with zero performance benefit.
--   Rule of thumb: use TABLE for small models, INCREMENTAL for large ones.
--
-- WHAT IS generate_surrogate_key()?
--   A dbt_utils macro that creates a deterministic MD5 hash from column values.
--   Example: generate_surrogate_key(['US-EAST']) → always the same hash.
--   WHY? Dimension tables need stable primary keys for joins. Using a hash
--   instead of the raw string gives you a consistent-length, compact key.
--
-- DATA FLOW:
--   fct_sales → [THIS MODEL] (aggregates fact data into region summaries)
-- ==============================================================================

{{
    config(
        materialized='table',            -- Full rebuild every run (small table)
        tags=['marts', 'dimension']      -- Dimension tag for selective execution
    )
}}

/*
    Region dimension table.

    Derives region attributes from transactional data and provides
    summary metrics useful for filtering and reporting.
*/

-- =============================================================================
-- CTE 1: Read from the fact table.
-- =============================================================================
-- NOTE: This dimension is DERIVED from the fact table (not from a source).
-- This is common for analytics dimensions that don't have a dedicated
-- source table. If you had a separate "regions" table in PostgreSQL,
-- you'd use source() instead.
with sales as (

    select * from {{ ref('fct_sales') }}

),

-- =============================================================================
-- CTE 2: Aggregate per region — one row per region with summary stats.
-- =============================================================================
-- These metrics are useful for:
--   - Dashboard filters ("show only regions with > 1000 customers")
--   - Data quality monitoring ("has this region stopped sending data?")
--   - Business analysis ("which region has the highest AOV?")
region_stats as (

    select
        region,
        count(distinct store_id)        as store_count,         -- How many stores in this region
        count(distinct customer_id)     as customer_count,      -- Unique customers served
        min(sale_date)                  as first_sale_date,     -- When data starts
        max(sale_date)                  as last_sale_date,      -- Most recent data
        count(*)                        as total_transactions,  -- Total sales count
        sum(total_amount)               as total_revenue,       -- Lifetime revenue
        avg(total_amount)               as avg_order_value      -- Average order value (AOV)
    from sales
    group by region

)

-- =============================================================================
-- FINAL SELECT — Build the dimension table with surrogate key.
-- =============================================================================
select
    -- Surrogate key: deterministic hash of the region name.
    -- {{ dbt_utils.generate_surrogate_key(['region']) }} compiles to:
    --   MD5(CAST(COALESCE(CAST(region AS VARCHAR), '_dbt_utils_surrogate_key_null_') AS VARCHAR))
    -- This creates a stable, compact primary key for joins.
    {{ dbt_utils.generate_surrogate_key(['region']) }} as region_key,

    region                      as region_name,         -- Human-readable region name
    store_count,                                        -- Count of stores
    customer_count,                                     -- Count of unique customers
    first_sale_date,                                    -- Earliest sale date
    last_sale_date,                                     -- Latest sale date
    total_transactions,                                 -- Total number of sales
    total_revenue::number(18, 2)    as total_revenue,   -- Explicit precision for consistency
    avg_order_value::number(10, 2)  as avg_order_value, -- Average order value
    current_timestamp()             as _loaded_at       -- When this dimension was last rebuilt
from region_stats
