# Fetch NASA APOD JSON and save to MinIO raw bucket
import os
import json
import datetime
from pathlib import Path
import requests
from dotenv import load_dotenv
import pandas as pd
import sys

# load .env from project root
project_root = Path(__file__).parent.parent.parent	
dotenv_path = project_root / ".env"
if dotenv_path.exists():
	load_dotenv(dotenv_path=dotenv_path)
else:
	load_dotenv()

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
			"endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
		},
	}
	
	# Validate MinIO config
	if not storage_options["key"] or not storage_options["secret"]:
		raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")

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
		print(f"MinIO endpoint: {storage_options['client_kwargs']['endpoint_url']}")
		print(f"MinIO bucket: {os.environ.get('MINIO_BUCKET')}")


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