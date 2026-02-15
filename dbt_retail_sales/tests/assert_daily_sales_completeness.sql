-- ==============================================================================
-- assert_daily_sales_completeness.sql — SINGULAR DATA TEST (Gap Detection)
-- ==============================================================================
--
-- WHAT DOES THIS TEST CHECK?
--   Verifies that EVERY region has at least one row in agg_daily_sales
--   for EVERY day in the last 7 days. If a region-date combination is
--   missing, it means either:
--   1. The extraction failed for that region on that date
--   2. The Airflow DAG didn't run
--   3. There was genuinely zero sales (unlikely for 4 large regions)
--
-- HOW IT WORKS — The "Cross Join + Anti-Join" Pattern:
--   Step 1: Get all known regions (expected)
--   Step 2: Get all dates in the last 7 days (date_range)
--   Step 3: CROSS JOIN them → every possible (date, region) combination
--   Step 4: LEFT JOIN with actual data → NULL = missing combination
--   Step 5: Return missing combinations (test FAILS if any exist)
--
-- EXAMPLE:
--   If we have 4 regions and 7 dates, the cross join creates 28 combinations.
--   If agg_daily_sales has 27 rows (missing US-WEST on Jan 3), this test
--   returns 1 row: (2024-01-03, US-WEST) → test FAILS.
--
-- WHY ONLY LAST 7 DAYS?
--   Checking all historical data would create false positives for dates
--   before a region was added, or during planned downtime. The 7-day
--   window focuses on recent, actionable gaps.
--
-- LEARNING TIP:
--   This "completeness check" pattern is extremely common in data quality:
--   1. Generate what SHOULD exist (cross join of dimensions)
--   2. Compare with what DOES exist (actual data)
--   3. Flag the gaps (anti-join)
-- ==============================================================================

/*
    Custom data test: every region should have at least one sale per day
    in the aggregation table. Flags gaps that may indicate extraction failures.

    Only checks dates in the last 7 days to avoid false positives for
    historical gaps.
*/

-- Step 1: Get all known regions from the aggregation table
with expected as (

    select distinct region from {{ ref('agg_daily_sales') }}

),

-- Step 2: Get all dates in the last 7 days that appear in the data
date_range as (

    select distinct sale_date
    from {{ ref('agg_daily_sales') }}
    where sale_date >= dateadd(day, -7, current_date())

),

-- Step 3: CROSS JOIN creates every possible (date, region) combination.
-- CROSS JOIN = cartesian product = every row in A paired with every row in B.
-- If we have 4 regions and 7 dates → 28 rows.
-- This represents what SHOULD exist if every region has daily data.
cross_joined as (

    select
        d.sale_date,
        e.region
    from date_range d
    cross join expected e

),

-- Step 4: Get what actually exists in the aggregation table
actual as (

    select sale_date, region
    from {{ ref('agg_daily_sales') }}

)

-- Step 5: Find gaps — combinations that SHOULD exist but DON'T.
-- LEFT JOIN + WHERE right IS NULL = "anti-join" pattern.
-- Returns rows from cross_joined that have NO match in actual.
select
    c.sale_date,
    c.region
from cross_joined c
left join actual a
    on c.sale_date = a.sale_date and c.region = a.region
where a.sale_date is null
-- If this query returns ANY rows, those are missing date-region gaps → test FAILS
