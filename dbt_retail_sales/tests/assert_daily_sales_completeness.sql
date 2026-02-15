/*
    Custom data test: every region should have at least one sale per day
    in the aggregation table. Flags gaps that may indicate extraction failures.

    Only checks dates in the last 7 days to avoid false positives for
    historical gaps.
*/

with expected as (

    select distinct region from {{ ref('agg_daily_sales') }}

),

date_range as (

    select distinct sale_date
    from {{ ref('agg_daily_sales') }}
    where sale_date >= dateadd(day, -7, current_date())

),

cross_joined as (

    select
        d.sale_date,
        e.region
    from date_range d
    cross join expected e

),

actual as (

    select sale_date, region
    from {{ ref('agg_daily_sales') }}

)

select
    c.sale_date,
    c.region
from cross_joined c
left join actual a
    on c.sale_date = a.sale_date and c.region = a.region
where a.sale_date is null
