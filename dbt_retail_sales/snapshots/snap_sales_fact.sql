-- ==============================================================================
-- snap_sales_fact.sql — SNAPSHOT (SCD Type 2 History)
-- ==============================================================================
--
-- WHAT IS A SNAPSHOT?
--   A dbt snapshot implements SCD Type 2 (Slowly Changing Dimension Type 2).
--   It tracks the FULL HISTORY of every record over time. When a record
--   changes, the snapshot:
--   1. Closes the OLD version (sets dbt_valid_to = current_timestamp)
--   2. Inserts the NEW version (with dbt_valid_from = current_timestamp)
--
-- WHAT IS SCD TYPE 2?
--   SCD (Slowly Changing Dimension) is a data warehousing concept for
--   tracking how dimension data changes over time:
--   - Type 1: Overwrite (no history — just UPDATE the row)
--   - Type 2: Add a new row with validity dates (full history) ← this
--   - Type 3: Add columns for old/new values (limited history)
--
-- COLUMNS ADDED BY dbt:
--   dbt automatically adds these columns to the snapshot table:
--   - dbt_scd_id:     Unique hash for each version of a record
--   - dbt_valid_from:  When this version became active
--   - dbt_valid_to:    When this version was superseded (NULL = current)
--   - dbt_updated_at:  The timestamp dbt used to detect the change
--
-- EXAMPLE:
--   Sale #1000 changes price from $10 to $15 on Jan 5:
--
--   | sale_id | amount | dbt_valid_from | dbt_valid_to   |
--   |---------|--------|----------------|----------------|
--   | 1000    | $10    | 2024-01-01     | 2024-01-05     |  ← old version
--   | 1000    | $15    | 2024-01-05     | NULL           |  ← current version
--
-- QUERY PATTERNS:
--   -- Current state (latest version of all records):
--   SELECT * FROM snap_sales_fact WHERE dbt_valid_to IS NULL
--
--   -- Point-in-time query ("what did the data look like on Jan 3?"):
--   SELECT * FROM snap_sales_fact
--   WHERE dbt_valid_from <= '2024-01-03' AND
--         (dbt_valid_to > '2024-01-03' OR dbt_valid_to IS NULL)
--
-- SNAPSHOT STRATEGIES:
--   - timestamp: Compare the updated_at column to detect changes
--                (fast, but requires a reliable timestamp column)
--   - check:     Compare specific column values to detect changes
--                (slower, but works without timestamps)
--
-- WHAT IS invalidate_hard_deletes?
--   When TRUE: if a row DISAPPEARS from the source (fct_sales), the snapshot
--   sets dbt_valid_to = current_timestamp on the last version, marking it
--   as historically closed. This is critical for CDC pipelines where
--   records are deleted from the fact table.
--   When FALSE: deleted rows stay "open" forever (dbt_valid_to = NULL).
--
-- RUN WITH:
--   dbt snapshot                    -- Run all snapshots
--   dbt snapshot -s snap_sales_fact  -- Run just this snapshot
--
-- DATA FLOW:
--   fct_sales → [THIS SNAPSHOT] → point-in-time queries, audit trails
--
-- LEARNING TIP:
--   Snapshots are perfect for:
--   - Audit trails (regulatory compliance)
--   - Price history tracking
--   - Customer address changes
--   - Any data where you need to answer "what was the value on date X?"
-- ==============================================================================

-- The {% snapshot %} block tells dbt this is a snapshot, not a regular model.
-- The name (snap_sales_fact) becomes the table name in the database.
{% snapshot snap_sales_fact %}

{{
    config(
        -- target_schema: which schema to create the snapshot table in.
        -- Snapshots live in their own schema, separate from marts.
        target_schema='snapshots',

        -- unique_key: the natural key that identifies a record across versions.
        -- dbt uses this to match "old" and "new" versions of the same record.
        unique_key='sale_id',

        -- strategy: how dbt detects changes.
        -- 'timestamp' compares the updated_at column between source and snapshot.
        -- If updated_at in the source is newer → record has changed → new version.
        strategy='timestamp',

        -- updated_at: which column to use for change detection.
        -- Must be a timestamp that changes whenever the record is modified.
        updated_at='updated_at',

        -- invalidate_hard_deletes: handle CDC deletes.
        -- When a row disappears from fct_sales (deleted by CDC post-hook),
        -- the snapshot closes the last version by setting dbt_valid_to.
        invalidate_hard_deletes=True
    )
}}

/*
    Snapshot (SCD Type 2) of the sales fact table.

    Tracks the full history of every sales record over time,
    enabling point-in-time analytics and audit trails.

    - strategy: timestamp (uses updated_at to detect changes)
    - invalidate_hard_deletes: marks rows as deleted when they
      disappear from the source (CDC delete propagation)
*/

-- SELECT * from the source model.
-- dbt compares each row's updated_at with the snapshot's stored version.
-- If different → insert new version and close old version.
select * from {{ ref('fct_sales') }}

{% endsnapshot %}
