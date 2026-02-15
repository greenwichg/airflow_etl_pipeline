{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
    Staging model: clean, cast, and deduplicate raw sales transactions.

    Business rules applied here:
    - Deduplication by sale_id (keep latest by updated_at)
    - NULL defaults for discount_amount and tax_amount
    - Type casting to canonical types
    - Filters out delete markers (handled by stg_cdc_delete_markers)
*/

with source as (

    select * from {{ source('raw_cdc', 'sales_data_stage') }}
    where etl_batch_id is not null

),

-- Keep only live rows (delete markers go to stg_cdc_delete_markers)
live_rows as (

    select *
    from source
    where is_deleted = false

),

-- Deduplicate: if the same sale_id appears multiple times in a batch,
-- keep the one with the latest updated_at (most recent change wins)
deduplicated as (

    select
        *,
        row_number() over (
            partition by sale_id
            order by updated_at desc, _cdc_source_ts desc nulls last
        ) as _row_num
    from live_rows

),

cleaned as (

    select
        -- Keys
        sale_id,
        customer_id,
        product_id,
        store_id,

        -- Timestamps
        sale_date::timestamp_ntz                        as sale_date,
        created_at::timestamp_ntz                       as created_at,
        updated_at::timestamp_ntz                       as updated_at,

        -- Measures (with null defaults)
        quantity::integer                                as quantity,
        unit_price::number(10, 2)                       as unit_price,
        total_amount::number(10, 2)                     as total_amount,
        coalesce(discount_amount, 0)::number(10, 2)     as discount_amount,
        coalesce(tax_amount, 0)::number(10, 2)          as tax_amount,

        -- Dimensions
        region,
        payment_method,

        -- CDC metadata
        is_deleted,
        _cdc_operation,
        _cdc_source_ts,

        -- ETL metadata
        etl_batch_id,
        etl_loaded_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
