USE ROLE ACCOUNTADMIN;

-- OPTIONAL: When we run query on Snowflake website, it will use the default COMPUTE_WH which has 10 mins suspend time.
-- Since Snowflake credit charge per warehouse active time so it's better to lower the suspend time.
ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH SET 
    WAREHOUSE_SIZE = 'X-SMALL' 
    AUTO_SUSPEND = 60; -- Change from 600 (10 mins) to 60 (1 min)

-- ==========================================
-- 1. INFRASTRUCTURE SETUP
-- ==========================================
-- A. Create the "Worker" (Compute Warehouse)
-- This is the engine that will process your data.
CREATE OR REPLACE WAREHOUSE DEMO_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60          -- Shuts down after 60s of inactivity to save money
    AUTO_RESUME = TRUE         -- Wakes up automatically when you run a query
    INITIALLY_SUSPENDED = TRUE; -- Don't start charging me until I actually run a query

-- B. Create the "Container" (Database)
CREATE OR REPLACE DATABASE DEMO_DE_PROJECT_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_DE_PROJECT_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS DEMO_DE_PROJECT_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS DEMO_DE_PROJECT_DB.GOLD;


-- c. Create User DATA_ENGINEER and grants USAGE privilege
CREATE ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE DEMO_WH TO ROLE DATA_ENGINEER;
GRANT ALL ON DATABASE DEMO_DE_PROJECT_DB TO ROLE DATA_ENGINEER;
-- The 'Office' key (Access to the existing schema)
GRANT ALL ON SCHEMA DEMO_DE_PROJECT_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL ON SCHEMA DEMO_DE_PROJECT_DB.GOLD TO ROLE DATA_ENGINEER;
-- This ensures that if you create a new  schema tomorrow, the role gets access automatically
GRANT ALL ON FUTURE SCHEMAS IN DATABASE DEMO_DE_PROJECT_DB TO ROLE DATA_ENGINEER;

SHOW GRANTS ON WAREHOUSE DEMO_WH;
GRANT ROLE DATA_ENGINEER TO USER ImaginysLight; -- In real world we need to create new user. But for demo just grant it to ourself
