# Fetch NASA APOD JSON for a given date and save it under data/nasa_apod/raw/<date>.json
import os
import json
import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv
import pandas as pd
import sys

# load .env 
dotenv_path = Path(__file__).parent / ".env"
if dotenv_path.exists():
	load_dotenv(dotenv_path=dotenv_path)


API_ENDPOINT = "https://api.nasa.gov/planetary/apod"


def fetch_apod(date: str | None = None, api_key: str | None = None):
	"""
	Fetch NASA APOD data for a given date and save to MinIO/S3
    Returns: S3 URI where the file was saved
	"""
	api_key = api_key or os.environ.get("NASA_API_KEY")
	if not api_key:
		raise RuntimeError("NASA_API_KEY not set. Export it (export NASA_API_KEY=your_key) or pass via api_key.")
	
	params = {"api_key": api_key}
	if date:
		params["date"] = date

	# Make API request
	resp = requests.get(API_ENDPOINT, params=params, timeout=15)
	resp.raise_for_status()
	data = resp.json()

	# Get date string for filename
	date_str = data.get("date") or date or datetime.date.today().isoformat()

	# MinIO/s3 Configuration
	s3_uri = f"s3://{os.environ.get('MINIO_BUCKET')}/raw/{date_str}.json"

	# storage options for MinIO
	storage_options = {
		"key": os.environ.get("MINIO_ACCESS_KEY"),
		"secret": os.environ.get("MINIO_SECRET_KEY"),
		"client_kwargs": {
			"endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://172.17.0.1:9000"),
		},
	
	}

	# Ensure the bucket exists before writing
	try:
		# Create DataFrame and save to S3/MinIO
		df = pd.DataFrame([data])
		df.to_json(
			s3_uri, 
			orient="records", 
			lines=False,
			storage_options=storage_options
		)
		print(f"Successfully saved raw APOD JSON to: {s3_uri}")
		return str(s3_uri)

	except Exception as e:
		print(f"Error saving to {s3_uri}: {e}")
		
		# Fallback to local storage
		local_dir = Path(__file__).parent.parent.parent / "tmp" / f"{date_str}.json"
		with open(local_dir, "w") as f:
			json.dump(data, f, indent=2)
		print(f"Saved raw APOD JSON locally to: {local_dir}")
		return str(local_dir)


if __name__ == "__main__":
	import argparse
	p = argparse.ArgumentParser()
	p.add_argument("--date", help="Date YYYY-MM-DD")
	args = p.parse_args()

	try:
		result = fetch_apod(date=args.date)
		print(f"Success: {result}")
	except Exception as e:
		print(f"Error: {e}")
		sys.exit(1)