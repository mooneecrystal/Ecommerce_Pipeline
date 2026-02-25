from airflow.sdk import dag, task
from datetime import timedelta, datetime
from src.generate_data import get_master_data, save_master_data, generate_new_customers, simulate_scd_updates, generate_daily_orders

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_generate_daily_data',
    default_args=default_args,
    description='Generates daily random data with chaos and uploads to MinIO',
    schedule='5 0 * * *', # At 00:05 every day
    start_date=datetime(2026, 1, 10),
    catchup=True,
    tags=['bronze', 'data_generation']
)
def generate_data_dag():
    @task
    def generate_and_upload_task(**context):
        execution_date = context['logical_date'].strftime("%Y-%m-%d")
        
        # 1. Load Master State (Persisted in src/data volume)
        df_customers = get_master_data('customers.csv', ['customer_id', 'signup_date'])
        df_products = get_master_data('products.csv', ['product_id'])
        
        # 2. Evolve State (Small daily growth)
        # We add 1 new customer per day in the live system
        df_customers = generate_new_customers(df_customers, execution_date, num_new=1)
        
        # SCD: Someone moves city
        df_customers = simulate_scd_updates(df_customers, execution_date, num_updates=1)
        
        # 3. Generate Orders
        # Uses the existing IDs + the 1 new guy
        df_orders, df_items = generate_daily_orders(
            execution_date, df_products, df_customers, num_orders=30
        )
        
        # 4. Upload
        from src.minio_utils import upload_df_to_s3
        upload_df_to_s3(df_customers, 'customers', execution_date, is_local=False)
        upload_df_to_s3(df_products, 'products', execution_date, is_local=False)
        upload_df_to_s3(df_orders, 'orders', execution_date, is_local=False)
        upload_df_to_s3(df_items, 'order_items', execution_date, is_local=False)
        
        # 5. Persist State (Crucial!)
        # So tomorrow's run sees the customer we just added.
        save_master_data(df_customers, 'customers.csv')
        # We didn't change products, but good practice to save if we did
        save_master_data(df_products, 'products.csv')

    generate_and_upload_task()

generate_data_dag()