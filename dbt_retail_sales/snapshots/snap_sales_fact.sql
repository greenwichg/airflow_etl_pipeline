{% snapshot snap_sales_fact %}

{{
    config(
        target_schema='snapshots',
        unique_key='sale_id',
        strategy='timestamp',
        updated_at='updated_at',
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

select * from {{ ref('fct_sales') }}

{% endsnapshot %}
