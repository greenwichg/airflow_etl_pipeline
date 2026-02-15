{{
    config(
        materialized='incremental',
        unique_key='sale_id',
        incremental_strategy='merge',
        merge_update_columns=[
            'customer_id', 'product_id', 'store_id', 'sale_date',
            'quantity', 'unit_price', 'total_amount', 'discount_amount',
            'tax_amount', 'net_amount', 'gross_profit', 'region',
            'payment_method', 'sale_year', 'sale_month', 'sale_day',
            'updated_at', 'etl_loaded_at', 'etl_batch_id',
            '_cdc_operation', '_cdc_source_ts', '_row_updated_at'
        ],
        cluster_by=['sale_date', 'region'],
        tags=['marts', 'daily', 'fact']
    )
}}

/*
    Fact table: validated sales transactions.

    Incremental strategy:
    - Full refresh: loads all validated records
    - Incremental: only processes new/changed records since last run
      (filtered by etl_loaded_at)

    CDC deletes are handled in a post-hook (see below).
*/

with validated as (

    select * from {{ ref('int_sales_validated') }}

    {% if is_incremental() %}
    -- Only process records loaded since the last dbt run
    where etl_loaded_at > (select coalesce(max(etl_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}

)

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

    -- Measures
    quantity,
    unit_price,
    total_amount,
    discount_amount,
    tax_amount,
    net_amount,
    gross_profit,

    -- Dimensions
    region,
    payment_method,

    -- Partition columns
    sale_year,
    sale_month,
    sale_day,

    -- CDC metadata
    _cdc_operation,
    _cdc_source_ts,

    -- ETL metadata
    etl_batch_id,
    etl_loaded_at,
    current_timestamp()     as _row_inserted_at,
    current_timestamp()     as _row_updated_at

from validated
