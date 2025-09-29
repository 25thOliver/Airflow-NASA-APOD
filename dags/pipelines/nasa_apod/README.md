# NASA APOD ETL

Place scripts under dags/pipelines/nasa_apod/. 
Airflow will import them inside the Docker container.

Dependencies (add to requirements.txt and rebuild container):
- requests
- pandas
