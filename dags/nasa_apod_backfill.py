from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Import paths
DAGS_DIR = Path(__file__).parent
if str(DAGS_DIR) not in sys.path:
    sys.path.append(str(DAGS_DIR))

# Import ETL function
from pipelines.nasa_apod.extract import fetch_apod
from pipelines.nasa_apod.transform import transform_apod_json
from pipelines.nasa_apod.load import append_staged_to_postgres

# DAG default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

"""
Backfill APOD data for a date range.
Dates are passed via DAG conf when triggering from Airflow UI.
"""
def _backfill(**context):
    conf = context["dag_run"].conf
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    if not start_date or not end_date:
        raise ValueError("Both start_date and end_date must be provided in DAG conf")
    
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    current =start
    while current <= end:
        date_str = current.isoformat()
        print(f"Processing APOD for {date_str}")

        raw_path = fetch_apod(date=date_str)
        staged_path = transform_apod_json(raw_path)
        append_staged_to_postgres(staged_path)

        current += timedelta(days=1)

    print(f"Backfill complete from {start_date} to {end_date}")

with DAG(
    dag_id="nasa_apod_backfill",
    description="One-off backfill DAG for NASA APOD",
    default_args=default_args,
    schedule=None, # no schedule -- run only when triggered manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nasa", "apod", "etl", "backfill"],
) as dag:
    
    backfill_task = PythonOperator(
        task_id="backfill",
        python_callable=_backfill,
    )