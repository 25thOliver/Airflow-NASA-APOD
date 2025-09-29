# Load a staged APOD record into PostgreSQL using Aiven connection string

import json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

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
    conn_string = os.environ.get("AIVEN_PG_CONN_STRING")
    if not conn_string:
        raise ValueError("AIVEN_PG_CONN_STRING not found in environment variables")

    # Check if the path is an S3 URI
    if staged_json_path.startswith("s3://"):
        print(f"Reading from S3: {staged_json_path}")

        # Storage options for MinIO/S3
        storage_options = {
            "key": os.environ.get("MINIO_ACCESS_KEY"),
            "secret": os.environ.get("MINIO_SECRET_KEY"),
            "client_kwargs": {
                "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://172.17.0.1:9000"),
            },
        }

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
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to staged JSON file")
    p.add_argument("--table", default="apod_records", help="Table name")

    args = p.parse_args()

    result = append_staged_to_postgres(args.input, args.table)
    print(f"Success: {result}")
