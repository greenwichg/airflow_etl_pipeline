-- ==============================================================================
-- agg_daily_sales.sql — MART MODEL (Incremental Aggregate Table)
-- ==============================================================================
--
-- WHAT IS AN AGGREGATE MODEL?
--   Aggregate models PRE-COMPUTE summary statistics that dashboards need.
--   Instead of BI tools running expensive GROUP BY queries on the fact table
--   (millions of rows) every time someone opens a dashboard, they query
--   this pre-aggregated table (thousands of rows) — orders of magnitude faster.
--
-- NAMING CONVENTION:
--   agg_<grain>_<entity>  →  agg_daily_sales
--   The "grain" is the level of aggregation (daily, by region).
--
-- WHY INCREMENTAL?
--   This table grows by ~4 rows per day (1 per region × 4 regions).
--   While a full rebuild would work, incremental is more efficient:
--   - Only re-aggregates dates that received new data
--   - Doesn't touch historical aggregations
--
-- COMPOSITE UNIQUE KEY:
--   unique_key=['sale_date', 'region'] — this is a COMPOSITE key.
--   It means there's exactly ONE row per date per region.
--   The MERGE matches on BOTH columns: if a row for 2024-01-15 + US-EAST
--   already exists, it gets UPDATED. Otherwise, a new row is INSERTED.
--
-- DATA FLOW:
--   fct_sales → [THIS MODEL] → dashboards / BI tools
-- ==============================================================================

{{
    config(
        materialized='incremental',
        -- COMPOSITE unique key: one row per (date, region) combination
        -- During MERGE, dbt matches on BOTH columns together
        unique_key=['sale_date', 'region'],
        incremental_strategy='merge',
        -- Cluster by sale_date for fast date-range queries on dashboards
        cluster_by=['sale_date'],
        tags=['marts', 'aggregate', 'daily']
    )
}}

/*
    Daily sales aggregation by region.

    Pre-computed rollup for dashboard KPIs:
    - Total revenue / orders / units
    - Average order value
    - Discount and tax totals
    - Net revenue and gross profit
    - Unique customer count

    Incremental: re-aggregates only dates that received new data.
*/

-- =============================================================================
-- Read from the fact table with optional incremental filter.
-- =============================================================================
with facts as (

    select * from {{ ref('fct_sales') }}

    {% if is_incremental() %}
    -- Only re-aggregate dates that have new data.
    -- Uses sale_date (not etl_loaded_at) because we're aggregating BY date.
    -- This means if a back-dated correction arrives, it re-aggregates that date.
    where sale_date >= (select coalesce(max(sale_date), '1900-01-01') from {{ this }})
    {% endif %}

),

-- =============================================================================
-- AGGREGATION — Compute daily KPIs per region.
-- =============================================================================
-- Each metric here answers a specific business question:
--   "How did each region perform yesterday?"
daily_agg as (

    select
        sale_date::date                             as sale_date,
        region,

        -- ===== VOLUME metrics (how much activity?) =====
        count(*)                                    as order_count,         -- Total orders
        sum(quantity)                               as total_units_sold,    -- Total items sold
        count(distinct customer_id)                 as unique_customers,    -- Distinct buyers
        count(distinct store_id)                    as active_stores,       -- Stores with sales

        -- ===== REVENUE metrics (how much money?) =====
        sum(total_amount)::number(18, 2)            as gross_revenue,      -- Before discounts
        sum(discount_amount)::number(18, 2)         as total_discounts,    -- Total discounts given
        sum(tax_amount)::number(18, 2)              as total_tax,          -- Total tax collected
        sum(net_amount)::number(18, 2)              as net_revenue,        -- After discounts
        sum(gross_profit)::number(18, 2)            as gross_profit,       -- After discounts + tax

        -- ===== AVERAGE metrics (what's typical?) =====
        avg(total_amount)::number(10, 2)            as avg_order_value,    -- AOV (key KPI)
        avg(quantity)::number(10, 2)                as avg_units_per_order,-- Items per order

        -- ===== PAYMENT MIX (how do customers pay?) =====
        -- These conditional aggregations count orders by payment method.
        -- Useful for: "What % of US-EAST orders are credit vs cash?"
        sum(case when payment_method = 'credit' then 1 else 0 end)
                                                    as credit_orders,
        sum(case when payment_method = 'cash' then 1 else 0 end)
                                                    as cash_orders,
        sum(case when payment_method = 'debit' then 1 else 0 end)
                                                    as debit_orders,

        -- ===== Metadata =====
        max(etl_batch_id)                           as latest_batch_id,    -- Most recent batch
        current_timestamp()                         as _aggregated_at      -- When this row was computed

    from facts
    group by sale_date::date, region

)

select * from daily_agg
