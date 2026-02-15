{% macro cdc_delete_post_hook(target_relation, delete_model, join_key='sale_id') %}
    /*
        Post-hook macro: delete rows from the target incremental table
        that have been marked as soft-deleted in the CDC pipeline.

        Usage in a model config:
            post_hook="{{ cdc_delete_post_hook(this, ref('stg_cdc_delete_markers')) }}"
    */
    delete from {{ target_relation }}
    where {{ join_key }} in (
        select {{ join_key }} from {{ delete_model }}
    )
{% endmacro %}
