from datetime import datetime
from airflow.sdk import dag, task

# CONFIG
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

@dag(
    dag_id='dag_build_analytic_table',
    schedule="@weekly",
    # start_date=datetime(2024, 2, 10),
    # end_date=datetime(2024, 4, 10),
    max_active_runs=1,
    catchup=False,
    tags=['gold', 'analytic', 'dbt']
)
def build_analytic_layer():
    
    @task.bash
    def dbt_monthly_product(ds=None):
        return f"{DBT_BIN} run --select agg_monthly_product --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
    
    @task.bash
    def dbt_best_brand(ds=None):
        return f"{DBT_BIN} run --select agg_best_brand --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
    
    @task.bash
    def dbt_customer_value(ds=None):
        return f"{DBT_BIN} run --select agg_customer_value --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
    
    @task.bash
    def dbt_quarter_profit(ds=None):
        return f"{DBT_BIN} run --select agg_quarter_profit --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --vars '{{\"execution_date\": \"{ds}\"}}'"
   
    run_monthly_product = dbt_monthly_product()
    run_best_brand = dbt_best_brand()
    run_customer_value = dbt_customer_value()
    run_quarter_profit = dbt_quarter_profit()

    [run_monthly_product, run_best_brand, run_customer_value, run_quarter_profit]

build_analytic_layer()