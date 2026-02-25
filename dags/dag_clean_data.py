from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# CONFIG
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

@dag(
    dag_id='dag_clean_data',
    schedule=None, # Change it to None because it's triggered by dag_ingest_data_to_snowflake automatically
    start_date=datetime(2024, 2, 10),
    max_active_runs=1,
    catchup=False,
    tags=['silver', 'snowflake', 'dbt']
)
def transform_data():

    # --- BLOCK 1: Customers  ---
    @task.bash
    def dbt_stg_customers(ds=None):
        # Run Silver & Quarantine for Customers
        return f"{DBT_BIN} run --select stg_customers stg_customers_quarantine --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"

    @task.bash
    def dbt_snap_customers(ds=None):
        return f"{DBT_BIN} snapshot --select scd_customers --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"

    # --- BLOCK 2: Products ---
    @task.bash
    def dbt_stg_products(ds=None):
        return f"{DBT_BIN} run --select stg_products stg_products_quarantine --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"

    @task.bash
    def dbt_snap_products(ds=None):
        return f"{DBT_BIN} snapshot --select scd_products --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
    
    # --- BLOCK 3: Orders and Order_items ---
    @task.bash
    def dbt_stg_orders(ds=None):
        return f"{DBT_BIN} run --select stg_orders stg_orders_quarantine --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"

    @task.bash
    def dbt_stg_order_items(ds=None):
        return f"{DBT_BIN} run --select stg_order_items stg_order_items_quarantine --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"

    # --- BLOCK 4: Quality test ---
    @task.bash
    def dbt_test_silver(ds=None):
        # We test ONLY the models we just built to save time
        return f"{DBT_BIN} test --select stg_customers stg_products stg_orders stg_order_items --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    
    # --- TRIGGER GOLD dag_transform_data  ---
    trigger_dag_transform_data = TriggerDagRunOperator(
        task_id="trigger_dag_transform_data",
        trigger_dag_id="dag_transform_data",  # The ID of the target DAG
        
        # Crucial: Pass the same date so the transformer knows which day to process
        logical_date="{{ logical_date }}",
        
        # If you re-run the generator, this forces the transformer to re-run too
        reset_dag_run=True,
        
        # False = "Fire and forget" (Generator finishes green immediately)
        # True = Generator stays "Running" until Transformer finishes (Not recommended)
        wait_for_completion=False 
    )

    # Instantiate the tasks
    cust_run = dbt_stg_customers()
    cust_snap = dbt_snap_customers()
    
    prod_run = dbt_stg_products()
    prod_snap = dbt_snap_products()

    order_run = dbt_stg_orders()
    order_items_run = dbt_stg_order_items()

    run_tests = dbt_test_silver()
    
    # 1. Run all Staging (Silver) in parallel
    [cust_run, prod_run, order_run, order_items_run] >> run_tests

    # 2. Only run Snapshots IF tests pass (Quality Gate)
    run_tests >> [cust_snap, prod_snap]

    # 3. Trigger the next DAG (Gold Layer)
    [cust_snap, prod_snap] >> trigger_dag_transform_data

transform_data()