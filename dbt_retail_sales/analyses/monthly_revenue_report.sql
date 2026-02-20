-- ==============================================================================
-- monthly_revenue_report.sql — ANALYSIS (Ad-Hoc Query)
-- ==============================================================================
--
-- WHAT IS A dbt ANALYSIS?
--   An analysis is a SQL file in the /analyses directory that is COMPILED
--   by dbt (Jinja is expanded) but NOT MATERIALIZED in the database.
--   No table or view is created — it's just a compiled SQL query.
--
-- WHY USE ANALYSES INSTEAD OF A REGULAR SQL FILE?
--   1. You can use ref() and source() — dbt resolves them to real table names
--   2. You can use macros and Jinja — variables, loops, conditionals
--   3. It's version-controlled in Git alongside your models
--   4. It appears in the dbt docs lineage graph
--
-- HOW TO USE:
--   1. Run `dbt compile` to expand Jinja into raw SQL
--   2. Find the compiled output in:
--      target/compiled/retail_sales/analyses/monthly_revenue_report.sql
--   3. Copy the compiled SQL and run it in your SQL client (Snowflake, DBeaver, etc.)
--
-- WHEN TO USE ANALYSES:
--   - Ad-hoc reports that don't need to be tables
--   - Data exploration queries shared with the team
--   - Audit queries (e.g., "show me suspicious transactions last week")
--   - Queries for stakeholders that benefit from Jinja templating
--
-- LEARNING TIP:
--   Think of analyses as "compiled SQL templates". They're NOT part of the
--   dbt run pipeline. You won't see them in `dbt run` or `dbt build`.
--   They exist purely for convenience and team collaboration.
-- ==============================================================================

-- Monthly revenue breakdown by region.
-- This analysis uses ref() to reference the fact table, so dbt will
-- resolve it to the actual schema.table_name during compilation.
select
    date_trunc('month', sale_date)::date     as month,
    region,

    -- Revenue metrics
    sum(net_amount)                          as monthly_revenue,
    sum(gross_profit)                        as monthly_profit,

    -- Volume metrics
    count(*)                                 as order_count,
    sum(quantity)                            as units_sold,
    count(distinct customer_id)              as unique_customers,

    -- Averages
    avg(net_amount)::number(10, 2)           as avg_order_value,

    -- Month-over-month growth (window function)
    -- LAG() gets the previous month's revenue for comparison
    lag(sum(net_amount)) over (
        partition by region
        order by date_trunc('month', sale_date)
    )                                        as prev_month_revenue,

    -- Growth percentage
    round(
        (sum(net_amount) - lag(sum(net_amount)) over (
            partition by region
            order by date_trunc('month', sale_date)
        )) / nullif(lag(sum(net_amount)) over (
            partition by region
            order by date_trunc('month', sale_date)
        ), 0) * 100, 2
    )                                        as mom_growth_pct

from {{ ref('fct_sales') }}
where sale_date >= dateadd(month, -12, current_date())
group by 1, 2
order by 1 desc, 2
