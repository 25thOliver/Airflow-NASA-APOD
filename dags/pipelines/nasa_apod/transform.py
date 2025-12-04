import json
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load .env
project_root = Path(__file__).parent.parent.parent
dotenv_path = project_root / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)

# Global storage options
storage_options = {
    "key": os.environ.get("MINIO_ACCESS_KEY"),
    "secret": os.environ.get("MINIO_SECRET_KEY"),
    "client_kwargs": {
        "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://172.17.0.1:9000"),
    },
    "config_kwargs": {
        "signature_version": "s3v4",
    }
}

def transform_apod_json(raw_s3_uri: str, staged_dir: str | None = None):
    """
    Transform raw APOD JSON from MinIO raw bucket into staged record and save to MinIO staged bucket.
    
    Args:
        raw_s3_uri: S3 URI to raw JSON file (e.g., s3://nasa-apod-dl/raw/2025-12-01.json)
        staged_dir: Optional staged directory URI (defaults to same bucket/staged/)
    
    Returns:
        S3 URI of the staged file
    """
    if not raw_s3_uri or not raw_s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid raw_s3_uri: {raw_s3_uri}. Must be an S3 URI starting with 's3://'")
    
    print(f"Transforming APOD data from: {raw_s3_uri}")
    
    # Read raw JSON from MinIO
    df = pd.read_json(raw_s3_uri, storage_options=storage_options)
    apod = df.iloc[0].to_dict()

    # Extract and clean fields for staged record
    record = {
        "date": apod.get("date"),
        "title": apod.get("title"),
        "explanation": apod.get("explanation"),
        "url": apod.get("hdurl") or apod.get("url"),  # Prefer HD URL if available
        "media_type": apod.get("media_type"),
        "service_version": apod.get("service_version"),
        "copyright": apod.get("copyright", "")
    }

    # Determine staged S3 URI
    date_str = str(record["date"]).split()[0] if " " in str(record["date"]) else record["date"]
    bucket_name = raw_s3_uri.split('/')[2]  # Extract bucket from s3://bucket/path
    staged_s3_uri = f"{staged_dir.rstrip('/')}/{date_str}.json" if staged_dir else f"s3://{bucket_name}/staged/{date_str}.json"
    
    # Save staged record to MinIO staged bucket
    pd.DataFrame([record]).to_json(staged_s3_uri, orient="records", storage_options=storage_options, index=False)
    print(f"Saved staged record to: {staged_s3_uri}")
    return str(staged_s3_uri)

def find_latest_apod_uri(base_uri: str):
    """
    Find the latest available APOD JSON file in the S3 bucket.
    """
    today = datetime.now()
    latest_date = None

    # Check up to 7 days back for available file
    for i in range(7):
        date_check = today - timedelta(days=i)
        date_str = date_check.strftime('%Y-%m-%d')
        raw_s3_uri = f"{base_uri}/raw/{date_str}.json"
        
        print(f"Checking for file: {raw_s3_uri}")  # Log the URI being checked
        
        try:
            # Try to read the file to check if it exists
            pd.read_json(raw_s3_uri, storage_options=storage_options)
            latest_date = date_str  # Update the latest date found
            break  # Exit the loop if the file was found
        except Exception:
            continue  # If the file does not exist, check the previous day

    if latest_date is None:
        raise FileNotFoundError("No valid APOD JSON files found.")

    # Construct the URI for the latest file
    return f"{base_uri}/raw/{latest_date}.json"

if __name__ == "__main__":
    base_uri = "s3://nasa-apod-dl"
    try:
        raw_s3_uri = find_latest_apod_uri(base_uri)
        staged_dir = f"{base_uri}/staged/"
        result = transform_apod_json(raw_s3_uri, staged_dir=staged_dir)
        print(f"Success: {result}")
    except Exception as e:
        print(f"Error: {e}")
        exit(1)