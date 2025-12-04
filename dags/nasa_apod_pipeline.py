import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# Fix for import paths 
# Ensure that the DAG folder itself is on sys.path
DAGS_DIR = Path(__file__).parent
if str(DAGS_DIR) not in sys.path:
    sys.path.append(str(DAGS_DIR))

# Import your ETL functions 
from pipelines.nasa_apod.extract import fetch_apod
from pipelines.nasa_apod.transform import transform_apod_json
from pipelines.nasa_apod.load import append_staged_to_postgres

# ETL Configuration
MINIO_BUCKET = "nasa-apod-dl"  # Bucket name for raw and staged data

# DAG default args 
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nasa_apod_pipeline",
    description="Daily ETL for NASA Astronomy Picture of the Day",
    default_args=default_args,
    schedule="@daily",   # run once per day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nasa", "apod", "etl"],
) as dag:

    def _extract(**context):
        date = (datetime.fromisoformat(context["ds"]) - timedelta(days=1)).date().isoformat()  
        try:
            return fetch_apod(date=date)
        except Exception as e:
            if "404" in str(e):
                raise AirflowSkipException(f"No APOD found for {date}")
            else:
                raise
       

    def _transform(**context):
        raw_path = context["ti"].xcom_pull(task_ids="extract")
        staged_dir = f"s3://{MINIO_BUCKET}/staged"
        return transform_apod_json(raw_path, staged_dir=staged_dir)

    def _load(**context):
        staged_path = context["ti"].xcom_pull(task_ids="transform")
        return append_staged_to_postgres(staged_path)

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=_load,
    )

    extract_task >> transform_task >> load_task
