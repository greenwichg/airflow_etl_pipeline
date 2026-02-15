-- ==============================================================================
-- dim_stores.sql — MART MODEL (Dimension Table)
-- ==============================================================================
--
-- WHAT DOES THIS MODEL DO?
--   Creates a STORE DIMENSION — one row per unique store_id with:
--   - Which region the store belongs to
--   - Activity dates (first and last sale)
--   - Summary metrics (transactions, revenue, unique customers)
--
-- WHY IS THIS USEFUL?
--   BI tools join fact tables with dimension tables to filter and aggregate:
--     SELECT d.region, SUM(f.total_amount)
--     FROM fct_sales f JOIN dim_stores d ON f.store_id = d.store_id
--     GROUP BY d.region
--
-- MATERIALIZATION:
--   TABLE — fully rebuilt every run. With thousands (not millions) of stores,
--   a full rebuild is cheap and ensures the metrics are always up to date.
--
-- DATA FLOW:
--   fct_sales → [THIS MODEL]
-- ==============================================================================

{{
    config(
        materialized='table',            -- Full rebuild (small dimension table)
        tags=['marts', 'dimension']
    )
}}

/*
    Store dimension derived from transactional data.

    Contains one row per unique store with its region,
    activity dates, and summary metrics.
*/

-- Read all sales from the fact table
with sales as (

    select * from {{ ref('fct_sales') }}

),

-- Aggregate one row per store with activity metrics
store_stats as (

    select
        store_id,
        region,                                         -- Which region this store belongs to
        min(sale_date)                  as first_sale_date,     -- First ever sale
        max(sale_date)                  as last_sale_date,      -- Most recent sale
        count(*)                        as total_transactions,  -- Lifetime sales count
        count(distinct customer_id)     as unique_customers,    -- How many different customers
        sum(total_amount)               as total_revenue,       -- Lifetime revenue
        avg(total_amount)               as avg_order_value      -- Average order value
    from sales
    group by store_id, region     -- Group by both to handle stores that move regions

)

-- Build the final dimension with surrogate key
select
    -- Surrogate key: MD5 hash of store_id for stable joins
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,

    store_id,                                           -- Natural key from source system
    region,                                             -- Region this store operates in
    first_sale_date,                                    -- First recorded sale
    last_sale_date,                                     -- Most recent sale
    total_transactions,                                 -- Total sales count
    unique_customers,                                   -- Distinct customer count
    total_revenue::number(18, 2)    as total_revenue,   -- Explicit precision
    avg_order_value::number(10, 2)  as avg_order_value, -- Average order value
    current_timestamp()             as _loaded_at       -- Dimension rebuild timestamp
from store_stats
