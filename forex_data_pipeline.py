from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("forex_data_pipeline", start_date=datetime(2021, 1 ,1),
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    #checks every 5 seconds (poke_interval) if the URL is available, and if it is returning what we expect
    is_forex_rates_available = HttpSensor(
        task_id='is_forex_rates_available',
        http_conn_id = 'forex_api',
        endpoint = 'marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b',
        response_check = lambda response: 'rates'in response.text,
        poke_interval = 5,
        timeout = 20
    )

    is_file_available = FileSensor(
        task_id = 'is_file_available',
        fs_conn_id = 'forex_path', #same id created in Airflow
        filepath = 'forex_currencies.csv', #name of the file to took for
        poke_interval = 5,
        timeout = 20
    )
