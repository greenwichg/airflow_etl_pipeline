{{
    config(
        materialized='ephemeral'
    )
}}

/*
    Apply data quality business rules to enriched sales data.

    Rules:
    1. quantity > 0
    2. unit_price > 0
    3. total_amount > 0
    4. Amount sanity check (computed in int_sales_enriched)
    5. Required fields are not null

    Records failing these rules are excluded from the fact table.
    In production, failed records would be routed to a quarantine table
    for investigation.
*/

with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

validated as (

    select
        *,
        case
            when quantity <= 0           then 'invalid_quantity'
            when unit_price <= 0         then 'invalid_price'
            when total_amount <= 0       then 'invalid_amount'
            when not is_amount_valid     then 'failed_amount_sanity'
            when customer_id is null     then 'missing_customer'
            when product_id is null      then 'missing_product'
            else null
        end as validation_failure_reason

    from enriched

),

-- Only pass records with no validation failures
passed as (

    select * from validated
    where validation_failure_reason is null

)

select * from passed
