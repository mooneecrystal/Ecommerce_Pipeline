USE DEMO_DE_PROJECT_DB;
-- ==========================================
-- 2. STAGE & FILE FORMAT SETUP
-- ==========================================

-- A. Create a File Format
-- This tells Snowflake how to read your CSVs (e.g., skip header row)
CREATE OR REPLACE FILE FORMAT BRONZE.MY_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', ''); -- Treat these strings as empty

-- B. Create the Internal Stage
-- This is the "landing zone" inside Snowflake where Airflow will PUT the files
CREATE OR REPLACE STAGE BRONZE.RAW_DATA_STAGE
    FILE_FORMAT = BRONZE.MY_CSV_FORMAT;

-- ==========================================
-- 3. BRONZE TABLE SETUP (The "Raw" Layer)
-- ==========================================
-- Note: All columns are VARCHAR (TEXT) to prevent load failures.
-- We add metadata columns (_filename, _loaded_at) for tracking.

-- 1. ORDERS
CREATE OR REPLACE TABLE BRONZE.ORDERS (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_date VARCHAR,
    order_status VARCHAR,
    revenue VARCHAR,
    discount VARCHAR,
    load_date VARCHAR,
    -- Metadata
    _filename VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. CUSTOMERS
CREATE OR REPLACE TABLE BRONZE.CUSTOMERS (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    signup_date VARCHAR,
    -- Metadata
    load_date VARCHAR,
    _filename VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. PRODUCTS
CREATE OR REPLACE TABLE BRONZE.PRODUCTS (
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    brand VARCHAR,
    unit_price VARCHAR,
    -- Metadata
    load_date VARCHAR,
    _filename VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. ORDER_ITEMS
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEMS (
    order_id VARCHAR,
    product_id VARCHAR,
    quantity VARCHAR,
    unit_price VARCHAR,
    -- Metadata
    load_date VARCHAR,
    _filename VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);