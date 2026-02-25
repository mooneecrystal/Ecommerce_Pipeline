{{ config(
    materialized='incremental',
    unique_key='_quarantine_id',
    tags=['quarantine', 'customers']
) }}

WITH hashed_source AS (
    SELECT 
        *,
        -- Generate a counter to ensure uniqueness even if 100 rows are missing IDs
        ROW_NUMBER() OVER (ORDER BY _loaded_at) as _rn
    FROM {{ source('internal_app', 'customers') }}
    -- Only process the bad rows
    WHERE 
        load_date = '{{ var("execution_date") }}' 
        AND {{ get_customer_quality_logic() }} IS NOT NULL
)

SELECT 
    * EXCLUDE _rn,
    
    MD5(CONCAT(
        COALESCE(customer_id, 'MISSING'), 
        load_date, 
        _rn::varchar
    )) as _quarantine_id,
    
    {{ get_customer_quality_logic() }} as quarantine_reason

FROM hashed_source
