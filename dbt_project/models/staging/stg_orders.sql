{{ config(
    materialized='incremental',
    unique_key='order_id',
    tags=['silver', 'orders']
) }}

WITH source AS (
    SELECT * FROM {{ source('internal_app', 'orders') }}
    {% if is_incremental() %}
        WHERE load_date = '{{ var("execution_date") }}'
    {% endif %}
),

cleaned AS (
    SELECT
        TRIM(order_id) as order_id,
        TRIM(customer_id) as customer_id, -- Don't check FK here. We will mark that customer as Unknown in fact tables.
        
        TRY_TO_DATE(order_date) as order_date,
        
        CASE
            WHEN UPPER(TRIM(order_status)) IN ('COMPLETED', 'PENDING', 'CANCELLED') 
                THEN UPPER(TRIM(order_status))
                ELSE 'UNKNOWN'
        END as order_status,

        TRY_CAST(REPLACE(REPLACE(revenue, '$', ''), ',', '') AS DECIMAL(10, 2)) as revenue,
        COALESCE(TRY_CAST(REPLACE(REPLACE(discount, '$', ''), ',', '') AS DECIMAL(10, 2)), 0.00) as discount,
        revenue - discount as net_profit,

        load_date,
        _filename,
        _loaded_at,

        {{ get_order_quality_logic() }} as _quarantine_reason

    FROM source
    -- Deduplicate: If the same Order ID sends an update in the same batch, take the latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1
)

SELECT * EXCLUDE _quarantine_reason 
FROM cleaned
WHERE _quarantine_reason IS NULL