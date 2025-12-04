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
    Transform raw APOD JSON from S3 into staged record and save back to S3.
    """
    try:
        # Read the raw JSON from S3
        if not raw_s3_uri:
            raise ValueError("raw_s3_uri is empty or None")
        print(f"Transformiong APOD data from: {raw_s3_uri}")
        
        if raw_s3_uri.startswith("s3://"):
            print("reading from S3/MinIO")
            df = pd.read_json(raw_s3_uri, storage_options=storage_options)
        else:
            print("reading from local file")
            df = pd.read_json(raw_s3_uri)
        apod = df.iloc[0].to_dict()

        # Prepare staged record
        record = {
            "date": apod.get("date"),
            "title": apod.get("title"),
            "explanation": apod.get("explanation"),
            "url": apod.get("hdurl") or apod.get("url"),
            "media_type": apod.get("media_type"),
            "service_version": apod.get("service_version"),
            "copyright": apod.get("copyright", "")
        }

        # Determine staged S3 URI
        date_str = str(record["date"]).split()[0] if " " in str(record["date"]) else record["date"]
        if raw_s3_uri.startswith("s3://"):
            staged_s3_uri = f"{staged_dir.rstrip('/')}/{date_str}.json" if staged_dir else f"s3://{raw_s3_uri.split('/')[2]}/staged/{date_str}.json"
            # Save staged record back to S3
            pd.DataFrame([record]).to_json(staged_s3_uri, orient="records", storage_options=storage_options, index=False)
        else:
            # Fallback: save to local file if raw was local
            staged_s3_uri = str(Path(raw_s3_uri).parent.parent / "staged" / f"{date_str}.json")
            Path(staged_s3_uri).parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame([record]).to_json(staged_s3_uri, orient="records", index=False)
        print(f"Saved staged record to: {staged_s3_uri}")
        return str(staged_s3_uri)

    except Exception as e:
        print(f"Error transforming APOD data: {e}")
        raise

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