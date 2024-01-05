import sys
sys.path.insert(0, '/usr/local/airflow/.local/lib/python3.7/site-packages')

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from stream_to_kafka import start_streaming


start_date = datetime(2023, 1, 1)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('nasa_apod',
         default_args=default_args,
         schedule_interval='0 6 * * *',
         catchup=False) as dag:

    data_stream_task = PythonOperator(
        task_id='kafka_data_stream',
        python_callable=start_streaming,
        dag=dag,
    )

    data_stream_task
