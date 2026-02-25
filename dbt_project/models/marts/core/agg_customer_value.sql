{{ config(
    materialized='table',
    tags=['gold', 'core']
) }}

WITH customer_sales AS (
    -- Step 1: Calculate LTV using ALL historical records (Do NOT filter by current_record here)
    SELECT 
        c.customer_id,
        SUM(fs.total_price) as gross_revenue,
        SUM(fs.item_net_profit) as net_revenue,
        MIN(fs.order_date_key) as first_purchase_date,
        MAX(fs.order_date_key) as last_purchase_date,

        SUM(CASE WHEN LEFT(CAST(fs.order_date_key AS VARCHAR), 4) = '2024' 
                 THEN fs.item_net_profit ELSE 0 END) as spend_2024,
        SUM(CASE WHEN LEFT(CAST(fs.order_date_key AS VARCHAR), 4) = '2025' 
                 THEN fs.item_net_profit ELSE 0 END) as spend_2025,
        SUM(CASE WHEN LEFT(CAST(fs.order_date_key AS VARCHAR), 4) = '2026' 
                 THEN fs.item_net_profit ELSE 0 END) as spend_2026
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('dim_customers') }} c ON fs.customer_pk = c.customer_pk
    WHERE fs.order_status = 'COMPLETED' 
      AND c.customer_id <> '-1'
    GROUP BY c.customer_id
),

current_profiles AS (
    -- Step 2: Get the most up-to-date address and name for the dashboard
    SELECT 
        customer_id, 
        full_name, 
        country, 
        state, 
        city
    FROM {{ ref('dim_customers') }}
    WHERE is_current_record = TRUE
)

-- Step 3: Combine the Math with the Current Profile
SELECT 
    p.customer_id, 
    p.full_name, 
    p.country, 
    p.state, 
    p.city,
    s.gross_revenue, 
    s.net_revenue, 
    s.first_purchase_date, 
    s.last_purchase_date, 
    s.spend_2024, 
    s.spend_2025, 
    s.spend_2026
FROM current_profiles p
JOIN customer_sales s ON p.customer_id = s.customer_id