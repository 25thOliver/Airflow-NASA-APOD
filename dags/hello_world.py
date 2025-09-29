from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
):
    @task
    def say_hello():
        print("Hello from Airflow 3.x! 🎉")

    say_hello()

