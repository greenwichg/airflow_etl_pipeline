{{
    config(
        materialized='view',
        tags=['staging', 'cdc']
    )
}}

/*
    Extracts CDC delete markers from the staging table.

    These are rows where is_deleted = TRUE, meaning the source system
    soft-deleted the transaction. The incremental mart model uses these
    to remove corresponding rows from the fact table.
*/

select distinct
    sale_id,
    region,
    updated_at,
    _cdc_operation,
    _cdc_source_ts,
    etl_batch_id
from {{ source('raw_cdc', 'sales_data_stage') }}
where is_deleted = true
  and etl_batch_id is not null
