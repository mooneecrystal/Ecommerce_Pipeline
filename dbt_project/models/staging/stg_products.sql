{{ config(
    materialized='table',
    tags=['silver', 'products']
) }}

WITH source AS (
    SELECT * FROM {{ source('internal_app', 'products') }}
),

cleaned AS (
    SELECT
        TRIM(product_id) as product_id,

        INITCAP(TRIM(product_name)) as product_name,
        INITCAP(TRIM(category)) as category,
        INITCAP(TRIM(subcategory))as subcategory,
        INITCAP(TRIM(brand)) as brand,

        TRY_CAST(REPLACE(REPLACE(unit_price, '$', ''), ',', '') AS DECIMAL(10, 2)) as unit_price,

        load_date,
        _filename,
        _loaded_at,

        -- Macro for the "Hard Block"
        {{ get_product_quality_logic() }} as _quarantine_reason

    FROM source
    -- Deduplicate within the TODAY'S data (if there were multiple files in the same folder)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) = 1
)

SELECT 
    * EXCLUDE _quarantine_reason
FROM cleaned
WHERE _quarantine_reason IS NULL