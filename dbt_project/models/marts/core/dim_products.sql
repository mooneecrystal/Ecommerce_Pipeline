{{ config(
    materialized='table',
    tags=['gold', 'core']
) }}

WITH scd_data AS (
    SELECT * FROM {{ ref('scd_products') }}
)

SELECT
    -- 1. Surrogate Key (Unique for every price/category/brand version)
    MD5(CONCAT(product_id, '-', dbt_valid_from)) as product_pk,

    -- 2. Natural Key
    product_id,

    -- 3. Attributes
    product_name,
    category,
    subcategory,
    brand,
    unit_price,

    -- 4. SCD Metadata
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dbt_valid_from ASC) as version,
    
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END as is_current_record

FROM scd_data

UNION ALL

-- Dummy '-1' for unknown situation
SELECT 
    '-1' as product_pk,
    '-1' as product_id,
    'Unknown Product' as product_name,
    'Missing' as category,
    'Missing' as subcategory,
    'Missing' as brand,
    0.00 as unit_price,
    CAST('1900-01-01' AS TIMESTAMP) as valid_from,
    NULL as valid_to,
    1 as version,
    TRUE as is_current_record