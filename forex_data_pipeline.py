from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }

    #in the file we have various pairs , depending on the base currency.
    #we want to download all the pairs for the base currencies.
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';') #we read the file
        for idx, row in enumerate(reader):
            base = row['base'] #base = EUR
            with_pairs = row['with_pairs'].split(' ') #with_pairs = [USD,NZD,JPY,GBP,CAD]
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json() #call the API with the correct ENDPOINT matching base variable
            '''indata = {
                "rates":{"CAD":1.31,"GBP":0.76,"JPY":108.56,"EUR":0.90,"NZD":1.52,"USD":1.0},
                "base":"EUR",
                "date":"2021-01-01"
                }'''
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            '''outdata = {
                base: EUR,
                rates: {},
                last_update = 2021-01-01
                }'''
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
                #we add the corresponding rate for pair in the list
                '''outdata = {
                base: EUR,
                rates: {CAD:1.31,...},
                last_update = 2021-01-01
                }'''
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                #write everything to a file
                json.dump(outdata, outfile)
                outfile.write('\n')


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

    download_rates_task = PythonOperator(
        task_id="downloading_rates",
        python_callable = download_rates #function to execute
    )

    saving_rates = BashOperator(
        task_id = 'saving_rates',
        bash_command = '''
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        '''
        #we create a folder forex, and move the json file into it
    )
