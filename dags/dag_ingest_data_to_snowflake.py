from datetime import datetime
from airflow.sdk import dag, task
from src.minio_utils import download_from_s3 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# CONFIG
DB_NAME = "DEMO_DE_PROJECT_DB"
SCHEMA = "BRONZE"
STAGE = "RAW_DATA_STAGE"

# DEFINING THE SCHEMA MAPPING
# Because we want a metadata column "_filename" in every table, we cannot directly COPY INTO from our .csv file
# We manually define how many columns each CSV has so we can build the SELECT statement dynamically
TABLE_CONFIG = {
    'orders': {
        'count': 6, 
        'cols': 'order_id, customer_id, order_date, order_status, revenue, discount'
    },
    'customers': {
        'count': 9, 
        'cols': 'customer_id, first_name, last_name, email, phone, city, state, country, signup_date'
    },
    'products': {
        'count': 6, 
        'cols': 'product_id, product_name, category, subcategory, brand, unit_price'
    },
    'order_items': {
        'count': 4, 
        'cols': 'order_id, product_id, quantity, unit_price'
    }
}

@dag(
    dag_id='dag_ingest_data_to_snowflake',
    schedule="@daily",
    start_date=datetime(2024, 2, 15),
    end_date=datetime(2024, 5, 15), # Adjust end_date for debug.
    max_active_runs=1,
    catchup=True,
    tags=['bronze', 's3', 'snowflake']
)
def ingest_pipeline_final():

    # --- TASK 1: DOWNLOAD ---
    @task
    def download_all_tables(ds=None):
        for table in TABLE_CONFIG.keys():
            s3_path = f"raw_data/{table}/load_date={ds}/"
            local_path = f"/tmp/data/{table}/load_date={ds}/"
            download_from_s3(s3_path, local_path)
        return "/tmp/data/"

    download_task = download_all_tables()

    load_tasks = []
    # --- TASK 2: LOAD LOOP ---
    for table, config in TABLE_CONFIG.items():
        
        # 1. Generate the $1, $2, $3... string based on column count
        # Example: if count is 3, this becomes "$1, $2, $3"
        csv_columns = ", ".join([f"${i+1}" for i in range(config['count'])])
        
        # 2. PUT Command
        put_query = f"""
            PUT file:///tmp/data/{table}/load_date={{{{ ds }}}}/*.csv 
            @{DB_NAME}.{SCHEMA}.{STAGE}/{table}/load_date={{{{ ds }}}}/ 
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE
        """

        # 3. COPY Command with Explicit Transformation
        # We Map: CSV Columns ($1..$N) -> Table Columns (col1..colN)
        # We Map: {ds} -> LOAD_DATE, Metadata -> _FILENAME
        # We Ignore: _LOADED_AT (It defaults to current timestamp automatically)
        copy_query = f"""
            COPY INTO {DB_NAME}.{SCHEMA}.{table.upper()}
            ( {config['cols']}, LOAD_DATE, _FILENAME ) 
            FROM (
                SELECT 
                    {csv_columns},  -- The data columns
                    '{{{{ ds }}}}',
                    METADATA$FILENAME 
                FROM @{DB_NAME}.{SCHEMA}.{STAGE}/{table}/load_date={{{{ ds }}}}/
            )
            FILE_FORMAT = (
                TYPE = 'CSV',
                SKIP_HEADER = 1, 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- To keep a .csv column contains a string with comma as one column
            )
            PATTERN = '.*.csv.gz'
        """

        load_task = SQLExecuteQueryOperator(
            task_id=f'load_{table}_to_snowflake',
            conn_id='snowflake_conn',
            sql=[put_query, copy_query],
            do_xcom_push=False,
            split_statements=False 
        )
        download_task >> load_task
        load_tasks.append(load_task)

    # --- TASK 3: TRIGGER SILVER dag_transform_data ---
    trigger_dag_clean_data = TriggerDagRunOperator(
        task_id="trigger_dag_clean_data",
        trigger_dag_id="dag_clean_data",  # The ID of the target DAG
        
        # Crucial: Pass the same date so the target knows which day to process
        logical_date="{{ logical_date }}",
        
        # If you re-run the generator, this forces the target to re-run too
        reset_dag_run=True,
        
        # False = "Fire and forget" (Generator finishes green immediately)
        # True = Generator stays "Running" until target finishes (Not recommended)
        wait_for_completion=False 
    )
    load_tasks >> trigger_dag_clean_data
    
ingest_pipeline_final()