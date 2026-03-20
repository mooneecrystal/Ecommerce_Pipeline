Hello, this is a tutorial project illustrating the pipeline for E-commerce data processing, it uses Airflow to orchestrating the data flow:
- Ingest the historical and new daily data from MinIO S3
- Push the data to Snowflake
- Use DBT to Clean and Transform data
- Connect the data to Power BI to draw statistical charts

Step-by-step guide is in the **SETUP.md** file, though I cannot go into all the details so it's better to have a chatbot to assist you to walkthrough.<br/>
The initial requirements below is written by Claude, which I will change it on the fly to adapt with the reality need.

# E-commerce Sales Data Warehouse reqirements
### Business Context
Build a dimensional data warehouse for e-commerce sales analysis. Transform transactional data into star schema for business intelligence. This demonstrates: data modeling, dimensional design, incremental loading, and analytics-ready data structures.

**Data Source:** Simulated e-commerce transactions (CSV exports or generate synthetic data)
- Orders, customers, products, order_items
- Incremental loads
- Historical data: 2 years

**Tech Stacks**
- Docker
- Airflows
- Snowflake
- DBT
- MinIO S3

**Concepts**
- Bronze, Silver, Golder layers
- Late data arrival
- Star Schema
- Slowly Changing Dimension (SCD)
- Column Custered Index
- Database Partition, Z-Order

### Project Tree
```
Demo DE Project/
├── docker-compose.yaml
├── Dockerfile
├── generate_history.ipynb
├── README.md
├── requirements.txt
├── SETUP.md
├── dags/
│   ├── dag_build_analytic_table.py
│   ├── dag_clean_data.py
│   ├── dag_generate_daily_data.py
│   ├── dag_ingest_data_to_snowflake.py
│   ├── dag_transform_data.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   ├── dbt_packages/
│   ├── macros/
│   ├── models/
│   ├── snapshots/
├── snowflake_init_scripts/
│   ├── bronze_init.sql
│   └── warehouse_init.sql
├── src/
│   ├── generate_data.py
│   ├── minio_utils.py
│   ├── project_config.py
│   └── data/
```
### Architecture

```
┌──────────────────────────────────────────┐
│         Source Systems (CSV)             │
│  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│  │ orders   │  │customers │  │products│  │
│  └──────────┘  └──────────┘  └────────┘  │
└──────────────────┬───────────────────────┘
                   │
                   ▼
         ┌─────────────────┐
         │  Staging Area   │
         │   (Bronze)      │
         └────────┬────────┘
                  │
                  ▼
         ┌─────────────────┐
         │ Data Cleaning   │
         │   (Silver)      │
         └────────┬────────┘
                  │
                  ▼
    ┌─────────────────────────────┐
    │    Data Warehouse (Gold)    │
    │                             │
    │  ┌──────────────────────┐   │
    │  │   Dimension Tables   │   │
    │  │  ┌────────────────┐  │   │
    │  │  │ dim_customer   │  │   │
    │  │  │ dim_product    │  │   │
    │  │  │ dim_date       │  │   │
    │  │  └────────────────┘  │   │
    │  └──────────────────────┘   │
    │                             │
    │  ┌──────────────────────┐   │
    │  │     Fact Table       │   │
    │  │  ┌────────────────┐  │   │
    │  │  │  fact_sales    │  │   │
    │  │  └────────────────┘  │   │
    │  └──────────────────────┘   │
    └─────────────┬───────────────┘
                  │
                  ▼
         ┌─────────────────┐
         │   BI Dashboard   │
         │   (Power BI)     │
         └──────────────────┘
```

### Database Schema

**Staging Tables (Bronze - exact replica of source):**

```sql
CREATE SCHEMA silver;
CREATE TABLE silver.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    order_status VARCHAR(50),
    total_amount DECIMAL(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.customers (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.products (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.order_items (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```sql
CREATE SCHEMA gold;

-- Date Dimension (dynamic pre-populated)
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE gold.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(201),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE,
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON gold.dim_customer(customer_id, is_current);

-- Product Dimension (SCD Type 1)
CREATE TABLE gold.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id ON gold.dim_product(product_id);
CREATE INDEX idx_dim_product_category ON gold.dim_product(category, subcategory);
```

**Fact Table (Gold):**

```sql
-- Sales Fact Table
CREATE TABLE gold.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    
    -- Foreign keys to dimensions
    customer_key INT REFERENCES gold.dim_customer(customer_key),
    product_key INT REFERENCES gold.dim_product(product_key),
    order_date_key INT REFERENCES gold.dim_date(date_key),
    
    -- Degenerate dimensions
    order_status VARCHAR(50),
    
    -- Measures (facts)
    quantity INT,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    gross_amount DECIMAL(10, 2),
    net_amount DECIMAL(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_customer ON gold.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON gold.fact_sales(product_key);
CREATE INDEX idx_fact_sales_date ON gold.fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_order ON gold.fact_sales(order_id);
```

**Aggregate Tables (Gold - for performance):**

```sql
-- Daily sales summary
CREATE TABLE gold.fact_sales_daily (
    date_key INT REFERENCES gold.dim_date(date_key),
    customer_key INT REFERENCES gold.dim_customer(customer_key),
    product_key INT REFERENCES gold.dim_product(product_key),
    
    total_orders INT,
    total_quantity INT,
    total_gross_amount DECIMAL(12, 2),
    total_discount_amount DECIMAL(12, 2),
    total_net_amount DECIMAL(12, 2),
    average_order_value DECIMAL(10, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date_key, customer_key, product_key)
);

-- Monthly sales by category
CREATE TABLE gold.fact_sales_monthly_category (
    year INT,
    month INT,
    category VARCHAR(100),
    
    total_orders INT,
    total_quantity INT,
    total_revenue DECIMAL(12, 2),
    unique_customers INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (year, month, category)
);
```


