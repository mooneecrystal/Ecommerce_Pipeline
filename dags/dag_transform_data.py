from datetime import datetime
from airflow.sdk import dag, task

# CONFIG
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

@dag(
    dag_id='dag_transform_data',
    schedule=None, # Change it to None because it's triggered by dag_clean_data automatically
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['gold', 'snowflake', 'dbt']
)
def build_gold_layer():

    # Define the dimensions we want to build
    dimensions = ['dim_date', 'dim_customers', 'dim_products']
    
    dim_tasks = []

    for dim in dimensions:
        @task.bash(task_id=f"dbt_{dim}")
        def run_dim_task(model_name=dim, ds=None):
            return f"{DBT_BIN} run --select {model_name} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
        
        # Instantiate and add to list
        dim_tasks.append(run_dim_task())
    
    @task.bash
    def dbt_test_dim(ds=None):
        # We test ONLY the models we just built to save time
        return f"{DBT_BIN} test --select dim_date dim_customers dim_products --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    
    @task.bash
    def dbt_fact_tables(ds=None):
        return f"{DBT_BIN} run --select fact_sales --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
    
    @task.bash
    def dbt_test_fact(ds=None):
        # We test ONLY the models we just built to save time
        return f"{DBT_BIN} test --select fact_sales --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    
    @task.bash
    def dbt_agg_tables(ds=None):
        return f"{DBT_BIN} run --select agg_customer_value agg_monthly_product --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    
    # 1. Instantiate the tasks
    fact_tables_run = dbt_fact_tables()
    test_dim_run = dbt_test_dim()
    test_fact_run = dbt_test_fact()
    agg_tables_run = dbt_agg_tables()

    # 2. Set Dependencies
    dim_tasks >> test_dim_run >> fact_tables_run >> test_fact_run >> agg_tables_run

build_gold_layer()