import sys
sys.path.insert(0, '/usr/local/airflow/.local/lib/python3.7/site-packages')

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from send_daily_picture import send_daily_picture_func

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('send_telegram_message',
         default_args=default_args,
         schedule_interval='0 7 * * *',
         catchup=False) as dag:

    telegram_send_task = PythonOperator(
        task_id='telegram_send_task',
        python_callable=send_daily_picture_func,
        dag=dag,
    )

    telegram_send_task
