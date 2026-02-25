-- Monthly Revenue by Product 

{{ config(
    materialized='table',
    tags=['gold', 'core']
) }}

-- Calculate product sales monthly by fiscal year
-- Remember that we can have the same product_id with many product_pk in case of SCD
-- Since this is aggregation table, better to keep the product_pk

SELECT p.product_id, p.product_pk, p.product_name, 
    SUM(fs.total_price) as gross_revenue, SUM(fs.item_net_profit) as net_revenue,
    p.category, p.brand, d.fiscal_year, d.fiscal_quarter, d.month, d.month_name, p.valid_from, p.valid_to, p.is_current_record

FROM  {{ ref('dim_products') }} p
    LEFT JOIN {{ ref('fact_sales') }} fs ON p.product_pk = fs.product_pk
    JOIN {{ ref('dim_date') }} d ON fs.order_date_key = d.date_key
WHERE fs.order_status = 'COMPLETED'
GROUP BY p.product_pk, d.fiscal_year, d.month, d.month_name, d.fiscal_quarter, p.product_name, p.category, p.brand, p.product_id, p.valid_from, p.valid_to, p.is_current_record
ORDER BY d.fiscal_year DESC, d.month ASC