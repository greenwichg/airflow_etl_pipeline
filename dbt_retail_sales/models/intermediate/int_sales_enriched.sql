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

with staged as (

    select * from {{ ref('stg_sales_transactions') }}

),

enriched as (

    select
        -- Keys
        sale_id,
        customer_id,
        product_id,
        store_id,

        -- Timestamps
        sale_date,
        created_at,
        updated_at,

        -- Original measures
        quantity,
        unit_price,
        total_amount,
        discount_amount,
        tax_amount,

        -- Derived measures
        (total_amount - discount_amount)::number(10, 2)     as net_amount,
        (total_amount - discount_amount - tax_amount)::number(10, 2)
                                                            as gross_profit,

        -- Partition columns (for Snowflake clustering)
        year(sale_date)                                     as sale_year,
        month(sale_date)                                    as sale_month,
        day(sale_date)                                      as sale_day,

        -- Dimensions
        region,
        payment_method,

        -- Business rule: amount sanity check
        -- total_amount should be between 50% and 200% of qty * unit_price
        case
            when total_amount >= (quantity * unit_price * 0.5)
             and total_amount <= (quantity * unit_price * 2.0)
            then true
            else false
        end                                                 as is_amount_valid,

        -- CDC metadata
        is_deleted,
        _cdc_operation,
        _cdc_source_ts,

        -- ETL metadata
        etl_batch_id,
        etl_loaded_at

    from staged

)

select * from enriched
