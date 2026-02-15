-- ==============================================================================
-- int_sales_validated.sql — INTERMEDIATE MODEL (Ephemeral)
-- ==============================================================================
--
-- WHAT DOES THIS MODEL DO?
--   Applies DATA QUALITY business rules to the enriched sales data.
--   Records that fail validation are EXCLUDED from the downstream fact table.
--   This is the "quality gate" — only clean, valid records pass through.
--
-- WHY VALIDATE IN A SEPARATE MODEL?
--   Separation of concerns:
--   - int_sales_enriched: adds derived columns (what the data means)
--   - int_sales_validated: checks data quality (is the data trustworthy?)
--   This makes it easy to modify validation rules without touching
--   the enrichment logic, and vice versa.
--
-- VALIDATION vs TESTING:
--   This model does ROW-LEVEL validation (filter out bad rows at runtime).
--   dbt tests (in _models.yml) do COLUMN-LEVEL validation (fail the build).
--   Both are important:
--   - dbt tests catch systemic issues (e.g., "all quantities are NULL")
--   - This model catches individual bad rows (e.g., "one row has qty = -1")
--
-- WHAT HAPPENS TO FAILED ROWS?
--   Currently: excluded (filtered out). The validation_failure_reason column
--   documents WHY each row failed, but failed rows don't reach the mart.
--   In production: you'd typically INSERT failed rows into a quarantine table
--   for data engineers to investigate.
--
-- DATA FLOW:
--   int_sales_enriched → [THIS MODEL] → fct_sales
--
-- LEARNING TIP:
--   The CASE WHEN pattern used here is a common "first match" validator.
--   It checks rules in priority order and returns the FIRST failure reason.
--   If no rule fails, validation_failure_reason is NULL (= passed).
-- ==============================================================================

-- Ephemeral: compiled as a CTE inside fct_sales, not materialized in DB.
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

-- Read from the enrichment model (which is also ephemeral).
-- dbt chains ephemeral models: this CTE wraps the enriched CTE.
with enriched as (

    select * from {{ ref('int_sales_enriched') }}

),

-- =============================================================================
-- VALIDATION LOGIC — Check each row against business rules.
-- =============================================================================
-- The CASE WHEN acts like a series of IF statements.
-- It checks rules in order and returns the FIRST failure reason.
-- If ALL rules pass, the result is NULL (no failure).
--
-- LEARNING TIP:
--   This pattern is called "first-match validation". The order matters:
--   - Check the most common/critical failures first
--   - Once a failure is found, stop checking (CASE WHEN short-circuits)
validated as (

    select
        *,
        case
            -- Rule 1: Quantity must be positive (no zero-quantity or negative orders)
            when quantity <= 0           then 'invalid_quantity'

            -- Rule 2: Unit price must be positive (free items should be handled separately)
            when unit_price <= 0         then 'invalid_price'

            -- Rule 3: Total amount must be positive
            when total_amount <= 0       then 'invalid_amount'

            -- Rule 4: Amount sanity check (from int_sales_enriched)
            -- is_amount_valid = false when total_amount is outside 50%-200% of qty * price
            when not is_amount_valid     then 'failed_amount_sanity'

            -- Rule 5: Required foreign keys must not be null
            when customer_id is null     then 'missing_customer'
            when product_id is null      then 'missing_product'

            -- All rules passed: no failure reason
            else null
        end as validation_failure_reason

    from enriched

),

-- =============================================================================
-- FILTER — Only pass records with no validation failures.
-- =============================================================================
-- Records where validation_failure_reason IS NULL are clean and valid.
-- Failed records are silently excluded (in production, you'd log them).
passed as (

    select * from validated
    where validation_failure_reason is null

)

select * from passed
