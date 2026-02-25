S3_BUCKET = "demodeproject"
S3_CONN_ID = "minio_conn" # We will set this in Airflow UI
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ENDPOINT_LOCAL = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"


import os
BRONZE_PATH = "raw_data"

# Data Persistence (The "OLTP Database" Simulation)
# This finds the absolute path to src/data to ensure Docker/Local compatibility
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DATA_DIR = os.path.join(BASE_DIR, 'data') # We will save master CSVs here

# Ensure directory exists
os.makedirs(DATA_DIR, exist_ok=True)