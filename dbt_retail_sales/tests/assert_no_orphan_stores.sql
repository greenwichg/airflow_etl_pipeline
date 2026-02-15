-- ==============================================================================
-- assert_no_orphan_stores.sql — SINGULAR DATA TEST (Custom Test)
-- ==============================================================================
--
-- WHAT IS A SINGULAR TEST?
--   A singular test (also called custom test) is a SQL file in the /tests
--   directory. It's a SELECT query that should return ZERO rows when the
--   data is correct. If it returns ANY rows, the test FAILS.
--
-- SINGULAR vs SCHEMA TESTS:
--   - Schema tests:   Declared in YAML (_models.yml), reusable, parameterized
--                     Examples: unique, not_null, accepted_values
--   - Singular tests: Written as SQL files, one-off, specific to your data
--                     Examples: referential integrity, business rule checks
--
-- WHAT DOES THIS TEST CHECK?
--   Referential integrity between fct_sales and dim_stores.
--   Every store_id in the fact table MUST exist in the dimension table.
--   If a store appears in sales but NOT in the dimension, it's an "orphan" —
--   meaning some data was lost or the dimension build is incomplete.
--
-- WHY IS THIS IMPORTANT?
--   If store_id 42 exists in fct_sales but NOT in dim_stores, then:
--   - JOIN queries will silently drop those sales rows
--   - Revenue reports will undercount
--   - Dashboards will show wrong numbers
--
-- WHEN COULD THIS HAPPEN?
--   1. dim_stores was built before new sales data arrived
--   2. A region was added to the fact table but not the dimension
--   3. The dimension's GROUP BY excluded some store_ids
--
-- HOW dbt RUNS THIS TEST:
--   dbt wraps your SQL in a COUNT(*) query:
--     SELECT COUNT(*) FROM (your_query) WHERE count > 0
--   If count > 0 → test FAILS. If count = 0 → test PASSES.
--
-- RUN WITH:
--   dbt test -s assert_no_orphan_stores
--
-- LEARNING TIP:
--   This pattern (LEFT JOIN + WHERE right.key IS NULL) is the standard way
--   to find orphan records. It's also called an "anti-join."
-- ==============================================================================

/*
    Custom data test: every store_id in fct_sales must exist in dim_stores.

    This catches referential integrity issues where a transaction references
    a store that was dropped from the dimension (e.g., due to a missing region).
*/

-- LEFT JOIN fact table to dimension table.
-- If a store_id exists in fct_sales but NOT in dim_stores,
-- d.store_id will be NULL → that's an orphan we want to catch.
select
    f.store_id,
    count(*) as orphan_count       -- How many sales reference this orphan store
from {{ ref('fct_sales') }} f
left join {{ ref('dim_stores') }} d
    on f.store_id = d.store_id
where d.store_id is null           -- Only keep rows with NO matching dimension
group by f.store_id
having count(*) > 0                -- Only return if there are actual orphans
-- If this query returns ANY rows, the test FAILS
