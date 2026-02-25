{{ config(
    materialized='table',
    tags=['gold', 'core']
) }}

WITH scd_data AS (
    SELECT * FROM {{ ref('scd_customers') }}
)

SELECT
    -- 1. Create a Surrogate Key (Primary Key for Gold)
    -- This is unique for every version of the customer
    MD5(CONCAT(customer_id, '-', dbt_valid_from)) as customer_pk,

    -- 2. Natural Key
    customer_id,

    -- 3. Attributes
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) as full_name,
    email,
    phone,
    city,
    state,
    country,
    signup_date,

    -- 4. SCD Meta Data
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dbt_valid_from ASC) as version,
    
    -- Helpful flag for BI tools like Tableau/PowerBI
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END as is_current_record

FROM scd_data

UNION ALL

-- Dummy '-1' for unknown situation
SELECT 
    '-1' as customer_pk,
    '-1' as customer_id,
    'Unknown' as first_name,
    'Unknown' as last_name,
    'Unknown Customer' as full_name,
    'N/A' as email,
    'N/A' as phone,
    'Missing' as city,
    'Missing' as state,
    'Missing' as country,
    CAST('1900-01-01' AS DATE) as signup_date,
    CAST('1900-01-01' AS DATE) as valid_from,
    NULL as valid_to,
    1 as version,
    TRUE as is_current_record