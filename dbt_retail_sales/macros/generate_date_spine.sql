-- ==============================================================================
-- generate_date_spine.sql — MACRO (Date Sequence Generator)
-- ==============================================================================
--
-- WHAT DOES THIS MACRO DO?
--   Generates a continuous sequence of dates between two dates.
--   This is essential for finding "gaps" in data — if you have sales for
--   Jan 1, Jan 3, but NOT Jan 2, this macro helps you detect the gap.
--
-- HOW IT WORKS (Snowflake-specific):
--   1. generator(rowcount => N) creates N rows (Snowflake table function)
--   2. seq4() generates sequential integers (0, 1, 2, 3, ...)
--   3. dateadd(day, seq4(), start_date) adds those integers to start_date
--   Result: one row per day from start_date to end_date
--
-- EXAMPLE USAGE:
--   WITH dates AS (
--       {{ generate_date_spine('2024-01-01', '2024-01-31') }}
--   )
--   SELECT * FROM dates
--
--   Output:
--     date_day
--     2024-01-01
--     2024-01-02
--     ...
--     2024-01-31
--
-- COMMON USE CASES:
--   1. Gap detection: LEFT JOIN date spine with sales → NULL = missing date
--   2. Zero-fill reports: ensure every date appears even with no sales
--   3. Time series analysis: need a complete date range for trends
--
-- NOTE:
--   dbt_utils also provides a date_spine macro. This is a simplified
--   Snowflake-native version. For cross-database compatibility, use
--   {{ dbt_utils.date_spine(datepart='day', start_date='...', end_date='...') }}
--
-- LEARNING TIP:
--   The generator() + seq4() pattern is Snowflake-specific. On BigQuery you'd
--   use GENERATE_DATE_ARRAY(), and on PostgreSQL you'd use generate_series().
--   dbt_utils abstracts these differences — that's why packages are valuable.
-- ==============================================================================

{% macro generate_date_spine(start_date, end_date) %}
    /*
        Generates a continuous sequence of dates between start_date and end_date.
        Useful for gap-filling daily aggregation tables.

        Parameters:
          start_date: First date in the sequence (inclusive)
          end_date:   Last date in the sequence (inclusive)
    */
    select
        -- dateadd(day, N, start) adds N days to the start date
        -- seq4() generates 0, 1, 2, ... up to rowcount
        dateadd(day, seq4(), '{{ start_date }}'::date) as date_day
    from table(
        -- generator creates exactly enough rows to cover the date range
        -- datediff calculates the number of days between start and end
        -- +1 to include both start and end dates
        generator(rowcount => datediff(day, '{{ start_date }}'::date, '{{ end_date }}'::date) + 1)
    )
{% endmacro %}
