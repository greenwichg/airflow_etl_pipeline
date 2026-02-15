{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Store dimension derived from transactional data.

    Contains one row per unique store with its region,
    activity dates, and summary metrics.
*/

with sales as (

    select * from {{ ref('fct_sales') }}

),

store_stats as (

    select
        store_id,
        region,
        min(sale_date)                  as first_sale_date,
        max(sale_date)                  as last_sale_date,
        count(*)                        as total_transactions,
        count(distinct customer_id)     as unique_customers,
        sum(total_amount)               as total_revenue,
        avg(total_amount)               as avg_order_value
    from sales
    group by store_id, region

)

select
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,
    store_id,
    region,
    first_sale_date,
    last_sale_date,
    total_transactions,
    unique_customers,
    total_revenue::number(18, 2)    as total_revenue,
    avg_order_value::number(10, 2)  as avg_order_value,
    current_timestamp()             as _loaded_at
from store_stats
