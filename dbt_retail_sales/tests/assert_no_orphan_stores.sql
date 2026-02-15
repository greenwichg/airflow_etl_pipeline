/*
    Custom data test: every store_id in fct_sales must exist in dim_stores.

    This catches referential integrity issues where a transaction references
    a store that was dropped from the dimension (e.g., due to a missing region).
*/

select
    f.store_id,
    count(*) as orphan_count
from {{ ref('fct_sales') }} f
left join {{ ref('dim_stores') }} d
    on f.store_id = d.store_id
where d.store_id is null
group by f.store_id
having count(*) > 0
