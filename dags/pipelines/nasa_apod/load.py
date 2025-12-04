# Load a staged APOD record into PostgreSQL using Postgres connection string

import json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables
project_root = Path(__file__).parent.parent.parent
dotenv_path = project_root / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()

# Storage options for MinIO/S3
storage_options = {
    "key": os.environ.get("MINIO_ACCESS_KEY"),
    "secret": os.environ.get("MINIO_SECRET_KEY"),
    "client_kwargs": {
        "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://172.17.0.1:9000"),
        },
}

def get_latest_staged_file(base_uri: str):
    """
    Get the latest staged APOD JSON file URI from MinIO.
    """
    today = datetime.now()
    latest_date = None

    # Check the last 7 days for staged files
    for i in range(7):
        date_check = today - timedelta(days=i)
        date_str = date_check.strftime('%Y-%m-%d')
        staged_s3_uri = f"{base_uri}/staged/{date_str}.json"
        
        # Check if the staged file exists
        try:
            pd.read_json(staged_s3_uri, storage_options=storage_options)
            latest_date = date_str  # Update if file exists
            break  # Exit once the latest file is found
        except Exception:
            continue  # If the file does not exist, check previous days

    if latest_date is None:
        raise FileNotFoundError("No valid staged APOD JSON files found.")

    return f"{base_uri}/staged/{latest_date}.json"

def append_staged_to_postgres(staged_json_path: str, table_name: str = "apod_records"):
    """
    Append a staged APOD JSON to PostgreSQL
    Args:
        staged_json_path: Path or S3 URI to the staged JSON file
        table_name: Name of the table to append to
    Returns:
        Name of the table where data was loaded
    """
    # Get connection string from environment
    conn_string = os.environ.get("POSTGRES_CONN_STRING")
    if not conn_string:
        raise ValueError("POSTGRES_CONN_STRING not found in environment variables")

    # Check if the path is an S3 URI
    if staged_json_path.startswith("s3://"):
        print(f"Reading from S3: {staged_json_path}")

        

        # Read from S3/MinIO
        df = pd.read_json(staged_json_path, storage_options=storage_options)
    else:
        print(f"Reading from local file: {staged_json_path}")
        # Read from local file
        with open(staged_json_path, 'r') as f:
            record = json.load(f)
        df = pd.DataFrame([record])

    # Connect to PostgreSQL
    engine = create_engine(conn_string)

    # Append to table (creates table if doesn't exist)
    df.to_sql(table_name, engine, if_exists='append', index=False)

    print(f"Loaded record into {table_name}")
    return table_name

if __name__ == "__main__":
    base_uri = "s3://nasa-apod-dl"
    try:
        # Get the latest staged file from MinIO
        staged_json_path = get_latest_staged_file(base_uri)
        print(f"Using staged file: {staged_json_path}")
        
        # Load the staged file into PostgreSQL
        result = append_staged_to_postgres(staged_json_path)
        print(f"Success: {result}")
    except Exception as e:
        print(f"Error: {e}")