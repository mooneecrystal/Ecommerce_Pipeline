
from io import StringIO
import boto3
import uuid
from botocore.client import Config
from src.project_config import *
import shutil

def get_s3_client(is_local=False):
    """Returns a boto3 client configured for MinIO."""
    endpoint = MINIO_ENDPOINT_LOCAL if is_local else MINIO_ENDPOINT
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1' # MinIO requires a region, even if dummy
    )

def upload_df_to_s3(df, table_name, load_date, is_local=False):
    """Uploads a DataFrame as CSV to MinIO Bronze layer."""
    s3 = get_s3_client(is_local)
    
    # Generate unique filename
    file_id = str(uuid.uuid4()).split('-')[0].upper()
    filename = f"{table_name}_{file_id}.csv"
    key = f"{BRONZE_PATH}/{table_name}/load_date={load_date}/{filename}"
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
    except Exception as e:
        # Create bucket if it doesn't exist (mostly for local run convenience)
        try:
            s3.create_bucket(Bucket=S3_BUCKET)
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
        except Exception as bucket_e:
            print(f"Error uploading: {bucket_e}")


def download_from_s3(s3_prefix, local_dir):
    """
    Downloads all files from an S3 folder to a local directory.
    Raises an Exception if no files are found or download fails.
    """
    try:
        # 1: CLEAN UP OLD DATA ---
        if os.path.exists(local_dir):
            print(f"Removing old data at {local_dir}")
            shutil.rmtree(local_dir)  # Deletes folder and all contents
        os.makedirs(local_dir, exist_ok=True)

        # 2: Get all .csv files path
        s3 = get_s3_client()
        
        print(f"Checking bucket '{S3_BUCKET}' prefix '{s3_prefix}'...")
        # 1. Check if files exist
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        
        if 'Contents' not in response:
            # RAISE error so Airflow marks task as FAILED
            raise FileNotFoundError(f"No files found at {s3_prefix}")

        # 3. Download Files
        download_count = 0
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.csv'):
                file_name = os.path.basename(file_key)
                local_path = os.path.join(local_dir, file_name)
                
                print(f"Downloading {file_key} -> {local_path}")
                s3.download_file(S3_BUCKET, file_key, local_path)
                download_count += 1
        
        if download_count == 0:
            print(f"Prefix exists but contains no .csv files: {s3_prefix}")
            
        return True # Success

    except Exception as e:
        # Re-raise the exception so Airflow stops
        print(f"Failed to download {s3_prefix}: {str(e)}")
        raise e