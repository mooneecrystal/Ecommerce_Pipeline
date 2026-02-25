{{ config(
    materialized='table',
    tags=['silver', 'customers']
) }}

WITH source AS (
    SELECT * FROM {{ source('internal_app', 'customers') }}
    WHERE load_date <= '{{ var("execution_date") }}'
    -- Partition Pruning: Priority today's "Full Snapshot" folder since the customers.csv contains all customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY load_date DESC) = 1
),

cleaned AS (
    SELECT
        TRIM(customer_id) as customer_id,
        
        -- Logic: If INITCAP fails (unlikely for strings), keep original
        COALESCE(INITCAP(TRIM(first_name)), first_name) as first_name,
        COALESCE(INITCAP(TRIM(last_name)), last_name) as last_name,
        
        LOWER(TRIM(email)) as email,
        
        -- Safe phone: strip junk, but if result is empty/null, keep original
        COALESCE(NULLIF(REGEXP_REPLACE(phone, '[^0-9]', ''), ''), phone) as phone,
        
        INITCAP(TRIM(city)) as city,
        UPPER(TRIM(state)) as state, 
        UPPER(TRIM(country)) as country,
        
        -- Resilience: Try to parse date, but keep string if it's "trash"
        COALESCE(TRY_TO_DATE(signup_date)::varchar, signup_date) as signup_date,

        load_date,
        _filename,
        _loaded_at,

        -- Macro for the "Hard Block" (Missing ID)
        {{ get_customer_quality_logic() }} as _quarantine_reason

    FROM source
    -- Deduplicate within the TODAY'S data (if there were multiple files in the same folder)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) = 1
)

SELECT 
    * EXCLUDE _quarantine_reason
FROM cleaned
WHERE _quarantine_reason IS NULL