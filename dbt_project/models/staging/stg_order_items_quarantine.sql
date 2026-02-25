{{ config(
    materialized='incremental',
    unique_key='_quarantine_id',
    tags=['quarantine', 'order_items']
) }}

WITH source AS (
    SELECT * FROM {{ source('internal_app', 'order_items') }}
    WHERE load_date = '{{ var("execution_date") }}'
),

hashed_source AS (
    SELECT 
        *,
        -- Generate a unique counter for this batch to prevent ID collisions
        -- If a file has 100 bad rows, this ensures they are 1...100
        ROW_NUMBER() OVER (ORDER BY _loaded_at) as _rn
    FROM source
    -- Filter: Only capture the rows that failed the quality check
    WHERE {{ get_order_items_quality_logic() }} IS NOT NULL
)

SELECT 
    * EXCLUDE _rn,
    
    -- Create a Unique ID for the Quarantine Table
    -- Structure: OrderID - ProductID - Date - RowNumber
    MD5(CONCAT(
        COALESCE(order_id, 'MISSING'), 
        '-', 
        COALESCE(product_id, 'MISSING'), 
        '-', 
        load_date, 
        '-', 
        _rn::varchar
    )) as _quarantine_id,
    
    {{ get_order_items_quality_logic() }} as quarantine_reason

FROM hashed_source