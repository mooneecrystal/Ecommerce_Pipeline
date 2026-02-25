{% macro get_customer_quality_logic() %}
    -- We should not quarantine other trash information, since if an ID exist, customer already create an account in the platform to place order.
    CASE 
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'Missing CUSTOMER_ID'
        ELSE NULL 
    END
{% endmacro %}

{% macro get_product_quality_logic() %}
    -- We quarantine the wrong products information since if it's wrong we better don't allow user to order it.
    CASE 
        WHEN PRODUCT_ID IS NULL OR TRIM(PRODUCT_ID) = '' THEN 'Missing PRODUCT_ID'
        WHEN PRODUCT_NAME IS NULL OR TRIM(PRODUCT_NAME) = '' THEN 'Missing NAME' 
        WHEN CATEGORY IS NULL OR TRIM(CATEGORY) = '' THEN 'Missing CATEGORY' 
        WHEN BRAND IS NULL OR TRIM(BRAND) = '' THEN 'Missing BRAND' 
        WHEN TRY_CAST(REPLACE(REPLACE(UNIT_PRICE, '$', ''), ',', '') AS DECIMAL(10, 2)) IS NULL THEN 'Invalid Format UNIT_PRICE'
        WHEN TRY_CAST(REPLACE(REPLACE(UNIT_PRICE, '$', ''), ',', '') AS DECIMAL(10, 2)) <= 0 THEN 'Negative or Zero UNIT_PRICE'
        ELSE NULL 
    END
{% endmacro %}

{% macro get_order_quality_logic() %}
    CASE 
        WHEN ORDER_ID IS NULL OR TRIM(ORDER_ID) = ''  THEN 'Missing ORDER_ID'
        WHEN TRY_CAST(REPLACE(REPLACE(REVENUE, '$', ''), ',', '') AS DECIMAL(10, 2)) IS NULL THEN 'Invalid Format REVENUE'
        WHEN TRY_CAST(REPLACE(REPLACE(REVENUE, '$', ''), ',', '') AS DECIMAL(10, 2)) <= 0 THEN 'Negative or Zero REVENUE'
        ELSE NULL 
    END
{% endmacro %}

{% macro get_order_items_quality_logic() %}
    CASE 
        WHEN ORDER_ID IS NULL OR TRIM(ORDER_ID) = '' THEN 'Missing ORDER_ID'
        WHEN TRY_CAST(REPLACE(REPLACE(UNIT_PRICE, '$', ''), ',', '') AS DECIMAL(10, 2)) IS NULL THEN 'Invalid Format UNIT_PRICE'
        WHEN TRY_CAST(REPLACE(REPLACE(UNIT_PRICE, '$', ''), ',', '') AS DECIMAL(10, 2)) <= 0 THEN 'Negative or Zero UNIT_PRICE'
        WHEN TRY_CAST(QUANTITY AS INT) IS NULL THEN 'Invalid Format QUANTITY'
        WHEN TRY_CAST(QUANTITY AS INT) <= 0 THEN 'Negative or Zero QUANTITY'
        ELSE NULL 
    END
{% endmacro %}

