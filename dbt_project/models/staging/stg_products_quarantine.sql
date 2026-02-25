{{ config(
    materialized='incremental',
    unique_key='_quarantine_id',
    tags=['quarantine', 'products']
) }}

WITH hashed_source AS (
    SELECT 
        *,
        -- Generate a counter to ensure uniqueness even if 100 rows are missing IDs
        ROW_NUMBER() OVER (ORDER BY _loaded_at) as _rn
    FROM {{ source('internal_app', 'products') }}
    -- Only process the bad rows
    WHERE 
        load_date = '{{ var("execution_date") }}' 
        AND {{ get_product_quality_logic() }} IS NOT NULL
)

SELECT 
    * EXCLUDE _rn,
    
    MD5(CONCAT(
        COALESCE(PRODUCT_ID, 'MISSING'), 
        load_date, 
        _rn::varchar
    )) as _quarantine_id,
    
    {{ get_product_quality_logic() }} as quarantine_reason

FROM hashed_source