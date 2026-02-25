{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    tags=['silver', 'order_items']
) }}

WITH source AS (
    SELECT * FROM {{ source('internal_app', 'order_items') }}
    {% if is_incremental() %}
        WHERE load_date = '{{ var("execution_date") }}'
    {% endif %}
),

cleaned_data AS (
    SELECT 
        TRIM(order_id) as order_id,
        TRIM(product_id) as product_id,
        
        TRY_CAST(quantity AS INT) as quantity,
        TRY_CAST(REPLACE(REPLACE(UNIT_PRICE, '$', ''), ',', '') AS DECIMAL(10, 2)) as unit_price,

        load_date,
        _loaded_at,
        _filename,

        {{ get_order_items_quality_logic() }} as _quarantine_reason

    FROM source
),

indexed_item AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY product_id, _loaded_at
        ) as item_index_in_order
        
    FROM cleaned_data
    WHERE _quarantine_reason IS NULL
)

SELECT 
    MD5(CONCAT(
        order_id, 
        COALESCE(product_id, 'UNK'), 
        item_index_in_order
    )) as order_item_id,

    order_id,
    product_id,
    quantity,
    unit_price,
    (quantity * unit_price) as total_price,
    item_index_in_order,
    load_date,
    _filename,
    _loaded_at

FROM indexed_item
-- Final safety check to deduplicate if the EXACT same file was loaded twice
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY _loaded_at DESC) = 1