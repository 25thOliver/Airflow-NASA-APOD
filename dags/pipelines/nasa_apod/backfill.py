"""
Fetch historical APOD records, stage them in MinIo, and load into Postgres
"""

import os
import datetime
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from load import append_staged_to_postgres

# Load env
project_root = Path(__file__).parent.parent.parent
dotenv_path = project_root / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)
else:
    load_dotenv()

API_ENDPOINT = "https://api.nasa.gov/planetary/apod"

def fetch_range(start_date: str, end_date: str):
    params = {
        "api_key": os.environ.get("NASA_API_KEY"),
        "start_date": start_date,
        "end_date": end_date,
    }
    resp =requests.get(API_ENDPOINT, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def transform(record: dict) -> dict:
    return {
        "date": record.get("date"),
        "title": record.get("title"),
        "explanation": record.get("explanation"),
        "url": record.get("url"),
    }

def backfill(days: int = 30):
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=days)

    print(f"Fetching APOD record {start_date} -> {end_date}")

    data = fetch_range(start_date.isoformat(), end_date.isoformat())
    print(f"Got {len(data)} records")

    staged_uris= []
    storage_options = {
        "key": os.environ.get("MINIO_ACCESS_KEY"),
        "secret": os.environ.get("MINIO_SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        },
    }
    bucket = os.environ.get("MINIO_BUCKET", "nasa-apod-dl")

    for record in data:
        clean = transform(record)
        date_str = clean["date"]

        s3_uri = f"s3://{bucket}/staged/{date_str}.json"
        pd.DataFrame([clean]).to_json(
            s3_uri,
            orient="records",
            lines=False,
            storage_options=storage_options,
        )
        print(f"Staged --> {s3_uri}")


        # Load into Postgres
        append_staged_to_postgres(s3_uri, table_name="apod_record")
        staged_uris.append(s3_uri)

    print(f"Backfilled {len(staged_uris)} APOD records")
    return staged_uris

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30, help="How many past days to backfill")
    args = p.parse_args()

    backfill(args.days)