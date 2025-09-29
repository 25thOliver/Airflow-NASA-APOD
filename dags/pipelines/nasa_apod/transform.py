"""
Read a raw APOD JSON (from extract.py) and produce a cleaned/staged JSON record
with the fields: date, title, explanation, url, media_type, copyright, service_version
Saved under data/nasa_apod/staged/<date>.json
"""

import json
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env
dotenv_path = Path(__file__).parent / ".env"
if dotenv_path.exists():
	load_dotenv(dotenv_path=dotenv_path)

def transform_apod_json(raw_s3_uri: str, staged_dir: str | None = None):
	"""
	Transform raw APOD JSON from S3 into staged record and save back to S3.
		Args:
			raw_s3_uri: S3 URI of the raw JSON file (e.g., s3://nasa-apod-dl/raw/2023-10-15.json)
			staged_dir: S3 directory for staged files (e.g., s3://nasa-apod-dl/staged/)

		Returns:
			S3 URI of the staged JSON file
	"""

	# Storage options for MinIO
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

	try:
		# Read the raw JSON from S3
		df = pd.read_json(raw_s3_uri, storage_options=storage_options)
		apod = df.iloc[0].to_dict() # Convert first row to dict

		# Choose best image URL available (hdurl if present else url)
		image_url = apod.get("hdurl") or apod.get("url")

	    # Prepare staged record
		record = {
			"date": apod.get("date"),
			"title": apod.get("title"),
			"explanation": apod.get("explanation"),
			"url": image_url,
			"media_type": apod.get("media_type"),
			"service_version": apod.get("service_version"),
			"copyright": apod.get("copyright", "")
		}

		# Determine staged S3 URI
		# Ensure date_str is just YYYY-MM-DD without time component
		date_str = str(record["date"]).split()[0] if " " in str(record["date"]) else record["date"]

		if staged_dir:
			staged_s3_uri = f"{staged_dir.rstrip('/')}/{date_str}.json"
		else:
			# Extract bucket from raw_s3_uri and use staged directory
			bucket = raw_s3_uri.split("/")[2]
			staged_s3_uri = f"s3://{bucket}/staged/{date_str}.json"

		# Save staged record back to S3
		staged_df = pd.DataFrame([record])
		staged_df.to_json(
			staged_s3_uri,
			orient="records",
			storage_options=storage_options,
			index=False
		)

		print(f"Saved stage record to: {staged_s3_uri}")
		return str(staged_s3_uri)

	except Exception as e:
		print(f"Error transforming APOD data: {e}")
		raise

if __name__ == "__main__":
	import argparse
	p = argparse.ArgumentParser(description="Transform raw APOD JSON from S3 into staged record.")
	p.add_argument("--input", required=True, help="S3 URI of raw JSON file (e.g., s3://nasa-apod-dl/raw/2023-10-15.json)")
	p.add_argument("--staged-dir", required=False, help="S3 directory for staged files (e.g., s3://nasa-apod-dl/staged/)")
	args = p.parse_args()

	try:
		result = transform_apod_json(args.input, staged_dir=args.staged_dir)
		print(f"Success: {result}")
	except Exception as e:
		print(f"Error: {e}")
		exit(1)
