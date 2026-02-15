-- ==============================================================================
-- cdc_delete_post_hook.sql — MACRO (CDC Delete Post-Hook)
-- ==============================================================================
--
-- WHAT IS A POST-HOOK?
--   A post-hook is SQL that runs AFTER a model finishes building.
--   It's configured in the model's config() block:
--     {{ config(post_hook="{{ cdc_delete_post_hook(...) }}") }}
--
--   dbt also supports pre-hooks (run BEFORE the model). Common uses:
--   - pre_hook:  GRANT permissions, create temp tables
--   - post_hook: Delete CDC rows, add indexes, GRANT SELECT
--
-- WHY DO WE NEED THIS?
--   dbt's incremental MERGE handles INSERTs and UPDATEs, but NOT DELETEs.
--   When a source record is soft-deleted (is_deleted = true), the MERGE
--   sees it as an INSERT (because it has no matching key) or ignores it.
--   This post-hook explicitly DELETEs those rows from the fact table.
--
-- HOW IT WORKS:
--   After fct_sales finishes its MERGE, this post-hook runs:
--     DELETE FROM fct_sales
--     WHERE sale_id IN (SELECT sale_id FROM stg_cdc_delete_markers)
--   This removes any rows that were soft-deleted in the source system.
--
-- PARAMETERS:
--   target_relation: The table to delete from (usually {{ this }})
--   delete_model:    The model containing delete markers (ref('stg_cdc_delete_markers'))
--   join_key:        The column to match on (default: 'sale_id')
--
-- USAGE IN A MODEL:
--   {{
--       config(
--           materialized='incremental',
--           post_hook="{{ cdc_delete_post_hook(this, ref('stg_cdc_delete_markers')) }}"
--       )
--   }}
--
-- LEARNING TIP:
--   This is the standard pattern for handling CDC deletes in dbt.
--   The alternative is to keep soft-deleted rows in the fact table
--   with an is_deleted flag, but that complicates all downstream queries
--   (every query needs WHERE is_deleted = false).
-- ==============================================================================

{% macro cdc_delete_post_hook(target_relation, delete_model, join_key='sale_id') %}
    /*
        Post-hook macro: delete rows from the target incremental table
        that have been marked as soft-deleted in the CDC pipeline.

        This runs AFTER the model's MERGE completes, cleaning up
        any rows that were deleted in the source system.

        Usage in a model config:
            post_hook="{{ cdc_delete_post_hook(this, ref('stg_cdc_delete_markers')) }}"
    */
    delete from {{ target_relation }}
    where {{ join_key }} in (
        select {{ join_key }} from {{ delete_model }}
    )
{% endmacro %}
