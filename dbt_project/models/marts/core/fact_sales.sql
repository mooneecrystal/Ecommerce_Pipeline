{{ config(
    materialized='incremental',
    unique_key='sales_pk',
    tags=['gold', 'core']
) }}

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        -- Look back 7 days to catch late-arriving orders
        WHERE load_date >= DATEADD(day, -7, TO_DATE('{{ var("execution_date") }}'))
    {% endif %}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS ( SELECT * FROM {{ ref('dim_customers') }} ),
products AS ( SELECT * FROM {{ ref('dim_products') }} )

SELECT
    -- 1. Sales Primary Key (Deterministic)
    MD5(CONCAT(items.order_id, '-', items.item_index_in_order)) as sales_pk,
    
    items.order_id,
    
    -- 2. Foreign Keys (Joining to the specific SCD version)
    COALESCE(c.customer_pk, '-1') as customer_pk,
    COALESCE(p.product_pk, '-1') as product_pk,
    COALESCE(CAST(TO_CHAR(orders.order_date, 'YYYYMMDD') AS INT), -1) as order_date_key,
    
    -- 3. Degenerate Dimension
    COALESCE(orders.order_status, 'UNKNOWN') as order_status,
    
    -- 4. Measures
    items.quantity,
    items.unit_price,
    items.total_price,
    -- Discount column in Orders table, we can implement a complicated weighted distribution to separate the discount for each order_items, 
    -- and also solve the penny problem caused by rounding, but for a tutorial project I don't do that here.
    -- Let's assume that we can believe in net_profit column in orders table,  and distribute it here
    CASE 
        -- Safety Check: Maybe there's an edge case for GIFT order that cost $0
        WHEN orders.revenue IS NULL OR orders.revenue = 0 THEN 0
        WHEN orders.net_profit IS NULL THEN 0
        ELSE CAST(
            (items.total_price / orders.revenue) * orders.net_profit 
            AS DECIMAL(10, 2)
        )
    END as item_net_profit,

    items.load_date

FROM items
LEFT JOIN orders 
    ON items.order_id = orders.order_id
-- Join to Customer SCD: Find the address they had when the order was placed
LEFT JOIN customers c
    ON orders.customer_id = c.customer_id
    AND orders.order_date >= c.valid_from
    AND (c.valid_to IS NULL OR orders.order_date < c.valid_to)
-- Join to Product SCD: Find the price/category at the time of order
LEFT JOIN products p
    ON items.product_id = p.product_id
    AND orders.order_date >= p.valid_from
    AND (p.valid_to IS NULL OR orders.order_date < p.valid_to)