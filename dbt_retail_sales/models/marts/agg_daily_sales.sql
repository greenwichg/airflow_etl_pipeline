{{
    config(
        materialized='incremental',
        unique_key=['sale_date', 'region'],
        incremental_strategy='merge',
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

with facts as (

    select * from {{ ref('fct_sales') }}

    {% if is_incremental() %}
    where sale_date >= (select coalesce(max(sale_date), '1900-01-01') from {{ this }})
    {% endif %}

),

daily_agg as (

    select
        sale_date::date                             as sale_date,
        region,

        -- Volume metrics
        count(*)                                    as order_count,
        sum(quantity)                               as total_units_sold,
        count(distinct customer_id)                 as unique_customers,
        count(distinct store_id)                    as active_stores,

        -- Revenue metrics
        sum(total_amount)::number(18, 2)            as gross_revenue,
        sum(discount_amount)::number(18, 2)         as total_discounts,
        sum(tax_amount)::number(18, 2)              as total_tax,
        sum(net_amount)::number(18, 2)              as net_revenue,
        sum(gross_profit)::number(18, 2)            as gross_profit,

        -- Averages
        avg(total_amount)::number(10, 2)            as avg_order_value,
        avg(quantity)::number(10, 2)                as avg_units_per_order,

        -- Payment mix
        sum(case when payment_method = 'credit' then 1 else 0 end)
                                                    as credit_orders,
        sum(case when payment_method = 'cash' then 1 else 0 end)
                                                    as cash_orders,
        sum(case when payment_method = 'debit' then 1 else 0 end)
                                                    as debit_orders,

        -- Metadata
        max(etl_batch_id)                           as latest_batch_id,
        current_timestamp()                         as _aggregated_at

    from facts
    group by sale_date::date, region

)

select * from daily_agg
