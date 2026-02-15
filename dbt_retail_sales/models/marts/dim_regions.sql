{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Region dimension table.

    Derives region attributes from transactional data and provides
    summary metrics useful for filtering and reporting.
*/

with sales as (

    select * from {{ ref('fct_sales') }}

),

region_stats as (

    select
        region,
        count(distinct store_id)        as store_count,
        count(distinct customer_id)     as customer_count,
        min(sale_date)                  as first_sale_date,
        max(sale_date)                  as last_sale_date,
        count(*)                        as total_transactions,
        sum(total_amount)               as total_revenue,
        avg(total_amount)               as avg_order_value
    from sales
    group by region

)

select
    {{ dbt_utils.generate_surrogate_key(['region']) }} as region_key,
    region                      as region_name,
    store_count,
    customer_count,
    first_sale_date,
    last_sale_date,
    total_transactions,
    total_revenue::number(18, 2)    as total_revenue,
    avg_order_value::number(10, 2)  as avg_order_value,
    current_timestamp()             as _loaded_at
from region_stats
