import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator


with DAG(
    dag_id = 'beza_service_public_api_dag',
    schedule = '@daily',
    start_date = datetime(2024, 3, 11),
    catchup = False,
    schedule_interval= '*/10 * * * * *'

) as dag:
    
    task_is_api_active = HttpSensor(
        task_id = 'is_api_active',
        http_conn_id='beza_service_public_api',
        endpoint='home-page-settings/'
    )

    task_get_api_response = SimpleHttpOperator(
        task_id='get_api_response',
        http_conn_id='beza_service_public_api',
        endpoint = 'home-page-settings/',
        method= 'GET',
        response_filter = lambda response: json.loads(response.text),  # A lambda function to process the HTTP response by converting it from JSON to a Python object using json.loads(response.text)
        log_response = True
    )